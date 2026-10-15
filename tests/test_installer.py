"""The agent-machine installer served at /install.sh, run end to end against a real server."""

from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from mcp_agent_mail.http import CLIENT_SCRIPTS_DIR
from tests.live_server import TOKEN

FAKE_BROWSER = r'''#!{python}
"""Plays the person approving this machine: sign in with the server token, approve, follow the redirect."""
import http.cookiejar, os, sys, urllib.error, urllib.parse, urllib.request
url = sys.argv[1]; base = url.split("/mail/")[0]
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k): return None
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()), NoRedirect)
def post(path, fields):
    try: return opener.open(base + path, urllib.parse.urlencode(fields).encode())
    except urllib.error.HTTPError as exc: return exc
post("/mail/login", {{"token": "{token}", "next": "/mail"}})
approved = post("/mail/auth/cli", dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)))
urllib.request.urlopen(approved.headers["Location"]).read()
'''


def _get(url: str) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, b""


def test_installer_and_client_scripts_are_public_and_exact(server_url: str) -> None:
    code, body = _get(f"{server_url}/install.sh")
    script = body.decode()
    assert code == 200 and f'SERVER="{server_url}"' in script
    assert '"__AGENT_MAIL_SERVER__"' not in script  # filled in...
    assert "*__AGENT_MAIL_SERVER*" in script  # ...without rewriting its own guard (lfr-tunnel #1684)
    assert subprocess.run(["sh", "-n"], input=script, text=True).returncode == 0  # valid shell

    for name in ("mail_wake.py", "agent_mail_files.py"):
        code, body = _get(f"{server_url}/client/{name}")
        assert code == 200 and body == (CLIENT_SCRIPTS_DIR / name).read_bytes()
    for name in ("check_inbox.sh", "..%2F..%2Fpyproject.toml", "mail_wake.pyc"):
        assert _get(f"{server_url}/client/{name}")[0] in (401, 404)
    assert _get(f"{server_url}/mail/api/events?timeout=0")[0] == 401  # everything else still needs auth


def test_unserved_installer_refuses_to_guess_a_server(tmp_path: Path) -> None:
    template = Path(__file__).resolve().parents[1] / "src" / "mcp_agent_mail" / "templates" / "install.sh"
    done = subprocess.run(["sh", str(template), "--project", "p", "--agent", "a"], capture_output=True, text=True)
    assert done.returncode == 1 and "doesn't know its server" in done.stderr


def test_install_end_to_end(server_url: str, tmp_path: Path) -> None:
    browser = tmp_path / "fake-browser"
    browser.write_text(FAKE_BROWSER.format(python=sys.executable, token=TOKEN))
    browser.chmod(0o755)
    home, repo = tmp_path / "home", tmp_path / "repo"
    home.mkdir()
    (repo / ".claude").mkdir(parents=True)
    (repo / ".claude" / "settings.local.json").write_text(json.dumps({"permissions": {"allow": ["Bash(ls)"]}}))
    _, body = _get(f"{server_url}/install.sh")
    done = subprocess.run(
        ["sh", "-s", "--", "--project", "github.com/owner/repo", "--agent", "BlueLake", "--write-hooks", "claude"],
        input=body.decode(), text=True, capture_output=True, cwd=repo, timeout=120,
        env={"PATH": os.environ["PATH"], "HOME": str(home), "BROWSER": str(browser)},
    )
    assert done.returncode == 0, done.stderr + done.stdout

    installed = home / ".agent-mail"
    assert (installed / "mail_wake.py").read_bytes() == (CLIENT_SCRIPTS_DIR / "mail_wake.py").read_bytes()
    config = home / ".config" / "agent-mail" / "github.com_owner_repo__BlueLake.json"
    settings = json.loads(config.read_text())
    assert settings["AGENT_MAIL_TOKEN"].startswith("amt_") and settings["AGENT_MAIL_AGENT"] == "BlueLake"
    assert stat.S_IMODE(config.stat().st_mode) == 0o600
    assert settings["AGENT_MAIL_TOKEN"] not in done.stdout + done.stderr  # never shown

    hooks = json.loads((repo / ".claude" / "settings.local.json").read_text())
    assert hooks["permissions"] == {"allow": ["Bash(ls)"]}  # existing settings kept
    stop = hooks["hooks"]["Stop"][0]["hooks"][0]
    assert stop["asyncRewake"] is True and stop["command"] == f"python3 {installed}/mail_wake.py wait --config {config}"
    assert "claude mcp add --transport http agent-mail" in done.stdout


@pytest.mark.parametrize("args", [["--agent", "a"], ["--project", "p"], ["--project", "p", "--agent", "a", "--write-hooks", "vim"]])
def test_installer_rejects_bad_arguments(server_url: str, args: list[str]) -> None:
    _, body = _get(f"{server_url}/install.sh")
    done = subprocess.run(["sh", "-s", "--", *args], input=body.decode(), text=True, capture_output=True)
    assert done.returncode == 2
