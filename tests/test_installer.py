"""The agent-machine installer served at /install.sh, run end to end against a real server."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import ModuleType

import pytest

from mcp_agent_mail.http import CLIENT_SCRIPTS_DIR
from tests.live_server import TOKEN, client_env

# The POSIX installer needs a POSIX shell; on Windows the PowerShell tests below cover installing.
posix_only = pytest.mark.skipif(os.name == "nt", reason="install.sh is for macOS, Linux and WSL")

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
    if os.name != "nt":
        assert subprocess.run(["sh", "-n"], input=script, text=True).returncode == 0  # valid shell

    for name in ("mail_wake.py", "agent_mail_files.py"):
        code, body = _get(f"{server_url}/client/{name}")
        assert code == 200 and body == (CLIENT_SCRIPTS_DIR / name).read_bytes()
    for name in ("check_inbox.sh", "..%2F..%2Fpyproject.toml", "mail_wake.pyc"):
        assert _get(f"{server_url}/client/{name}")[0] in (401, 404)
    assert _get(f"{server_url}/mail/api/events?timeout=0")[0] == 401  # everything else still needs auth


@posix_only
def test_unserved_installer_refuses_to_guess_a_server(tmp_path: Path) -> None:
    template = Path(__file__).resolve().parents[1] / "src" / "mcp_agent_mail" / "templates" / "install.sh"
    done = subprocess.run(["sh", str(template), "--project", "p", "--agent", "a"], capture_output=True, text=True)
    assert done.returncode == 1 and "doesn't know its server" in done.stderr


@posix_only
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
        env=client_env(HOME=str(home), BROWSER=str(browser)),
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


@posix_only
@pytest.mark.parametrize("args", [["--agent", "a"], ["--project", "p"], ["--project", "p", "--agent", "a", "--write-hooks", "vim"]])
def test_installer_rejects_bad_arguments(server_url: str, args: list[str]) -> None:
    _, body = _get(f"{server_url}/install.sh")
    done = subprocess.run(["sh", "-s", "--", *args], input=body.decode(), text=True, capture_output=True)
    assert done.returncode == 2


# ---------------------------------------------------------------- Windows: install.ps1 and hook forms (#19)

PWSH = shutil.which("pwsh") or shutil.which("powershell")


def test_powershell_installer_is_public_and_filled_in(server_url: str) -> None:
    code, body = _get(f"{server_url}/install.ps1")
    script = body.decode()
    assert code == 200 and f'$Server = "{server_url}"' in script
    assert '"__AGENT_MAIL_SERVER__"' not in script and '"*__AGENT_MAIL_SERVER*"' in script


@pytest.mark.skipif(PWSH is None, reason="needs PowerShell (pwsh)")
def test_powershell_install_end_to_end(server_url: str, tmp_path: Path) -> None:
    assert PWSH is not None
    browser = tmp_path / ("fake-browser.py" if os.name == "nt" else "fake-browser")
    browser.write_text(FAKE_BROWSER.format(python=sys.executable, token=TOKEN))
    browser.chmod(0o755)
    home, repo = tmp_path / "home", tmp_path / "repo"
    home.mkdir()
    (repo / ".claude").mkdir(parents=True)
    (repo / ".claude" / "settings.local.json").write_text(json.dumps({"permissions": {"allow": ["Bash(ls)"]}}))
    command = (
        f"& ([scriptblock]::Create((Invoke-RestMethod '{server_url}/install.ps1'))) "
        "-Project github.com/owner/repo -Agent BlueLake -WriteHooks claude"
    )
    env = client_env(HOME=str(home), USERPROFILE=str(home)) | {
        "AGENT_MAIL_HOME": str(home / ".agent-mail"),
        "AGENT_MAIL_CONFIG_DIR": str(home / ".config" / "agent-mail"),
        "BROWSER": f'"{sys.executable}" "{browser}" %s' if os.name == "nt" else str(browser),
    }
    done = subprocess.run([PWSH, "-NoProfile", "-NonInteractive", "-Command", command],
                          text=True, capture_output=True, cwd=repo, timeout=180, env=env)
    assert done.returncode == 0, done.stderr + done.stdout

    installed = home / ".agent-mail"
    assert (installed / "mail_wake.py").read_bytes() == (CLIENT_SCRIPTS_DIR / "mail_wake.py").read_bytes()
    config = home / ".config" / "agent-mail" / "github.com_owner_repo__BlueLake.json"
    settings = json.loads(config.read_text())
    assert settings["AGENT_MAIL_TOKEN"].startswith("amt_") and settings["AGENT_MAIL_AGENT"] == "BlueLake"
    assert settings["AGENT_MAIL_TOKEN"] not in done.stdout + done.stderr  # never shown
    hooks = json.loads((repo / ".claude" / "settings.local.json").read_text())
    assert hooks["permissions"] == {"allow": ["Bash(ls)"]}
    stop = hooks["hooks"]["Stop"][0]["hooks"][0]
    assert stop["asyncRewake"] is True
    if os.name == "nt":  # exec form: interpreter plus arguments, no shell
        assert stop["args"] == [str(installed / "mail_wake.py"), "wait", "--config", str(config)]
    else:
        assert "wait --config" in stop["command"]
    assert "claude mcp add --transport http agent-mail" in done.stdout


def _load_mail_wake() -> ModuleType:
    spec = importlib.util.spec_from_file_location("mail_wake_under_test", CLIENT_SCRIPTS_DIR / "mail_wake.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_windows_command_lines_quote_paths_for_every_shell(tmp_path: Path) -> None:
    mail_wake = _load_mail_wake()
    config = Path("C:/Users/Peter Richards/.config/agent-mail/x.json")
    line = mail_wake.command_line("watch", "--config", config, windows=True)
    # Interpreter bare (never quoted: PowerShell would read a quoted first word as a string),
    # then double-quoted forward-slash paths, which Git Bash, PowerShell and cmd read alike.
    first, rest = line.split(" ", 1)
    assert '"' not in first and " " not in first
    assert rest.endswith(' watch --config "C:/Users/Peter Richards/.config/agent-mail/x.json"')
    assert mail_wake.command_line("watch", "--config", Path("/home/p/x.json"), windows=False).startswith("python3 ")

    claude = mail_wake.hook_settings("claude", config, windows=True)
    stop = claude["hooks"]["Stop"][0]["hooks"][0]
    assert stop["command"] == sys.executable and stop["args"][1:] == ["wait", "--config", str(config)]
    assert "record-usage" in claude["statusLine"]["command"]


def test_hook_merge_replaces_earlier_agent_mail_hooks_and_keeps_others() -> None:
    mail_wake = _load_mail_wake()
    new = mail_wake.hook_settings("claude", Path("/home/p/new.json"), windows=False)
    theirs = {"hooks": [{"type": "command", "command": "notify-send done"}]}
    old_ours = {"hooks": [{"type": "command", "command": "python3 /old/mail_wake.py wait --config /old.json"}]}
    current = {"permissions": {"allow": ["Bash(ls)"]}, "statusLine": {"type": "command", "command": "my-status"},
               "hooks": {"Stop": [theirs, old_ours], "PreToolUse": [theirs]}}
    merged, notes = mail_wake.merge_hook_settings("claude", current, new)
    assert merged["permissions"] == current["permissions"] and merged["hooks"]["PreToolUse"] == [theirs]
    assert merged["hooks"]["Stop"] == [theirs, *new["hooks"]["Stop"]]  # old copy gone, theirs kept
    assert merged["statusLine"] == new["statusLine"] and any("my-status" in n for n in notes)
    again, notes = mail_wake.merge_hook_settings("claude", merged, new)
    assert again == merged and notes == []  # idempotent


def test_hook_merge_keeps_a_users_hook_that_shares_a_group_with_ours() -> None:
    mail_wake = _load_mail_wake()
    new = mail_wake.hook_settings("claude", Path("/home/p/new.json"), windows=False)
    mixed = {"hooks": [{"type": "command", "command": "notify-send done"},
                       {"type": "command", "command": "python3 /old/mail_wake.py wait --config /old.json"}]}
    merged, _ = mail_wake.merge_hook_settings("claude", {"hooks": {"Stop": [mixed]}}, new)
    assert merged["hooks"]["Stop"] == [{"hooks": [{"type": "command", "command": "notify-send done"}]}, *new["hooks"]["Stop"]]


windows_only = pytest.mark.skipif(os.name != "nt", reason="native Windows only")


@windows_only
def test_windows_hooks_run_from_a_folder_with_a_space_and_an_accent(tmp_path: Path) -> None:
    """The hooks as each client starts them on Windows, from a user folder like C:\\Users\\Pé Richards."""
    profile = tmp_path / "Pé Richards"
    home = profile / ".agent-mail"
    home.mkdir(parents=True)
    for name in ("mail_wake.py", "agent_mail_files.py"):
        shutil.copy(CLIENT_SCRIPTS_DIR / name, home / name)
    config = profile / ".config" / "github.com_owner_repo__BlueLake.json"
    config.parent.mkdir()
    # No server: the hooks run, read their input and stay silent, which is what's checked here.
    config.write_text(json.dumps({"AGENT_MAIL_PROJECT": "github.com/owner/repo", "AGENT_MAIL_AGENT": "BlueLake",
                                  "AGENT_MAIL_WAKE_STATE_DIR": str(tmp_path / "state")}), encoding="utf-8")
    repo = tmp_path / "repo"
    repo.mkdir()
    for client in ("claude", "antigravity"):
        done = subprocess.run([sys.executable, str(home / "mail_wake.py"), "hooks", client, "--config", str(config), "--write"],
                              cwd=repo, env=client_env(), capture_output=True, text=True, timeout=60)
        assert done.returncode == 0, done.stderr

    # Antigravity: `cmd /C <command>`, which mangles quotes, so the command is one bare short path.
    agy = json.loads((repo / ".agents" / "hooks.json").read_text(encoding="utf-8"))["agent-mail"]
    for event in ("PreInvocation", "Stop"):
        command = agy[event][0]["command"]
        assert " " not in command and command.isascii() and command.lower().endswith(".cmd"), command  # 8.3 names are upper-case
        ran = subprocess.run(f"cmd /C {command}", input=json.dumps({"conversationId": "c1"}), env=client_env(),
                             capture_output=True, text=True, timeout=60)
        assert ran.returncode == 0 and ran.stdout.strip() == "{}", (ran.stdout, ran.stderr)

    # Claude Code: exec form (no shell), and the status line through PowerShell (no Git Bash).
    claude = json.loads((repo / ".claude" / "settings.local.json").read_text(encoding="utf-8"))
    stop = claude["hooks"]["Stop"][0]["hooks"][0]
    ran = subprocess.run([stop["command"], *stop["args"]], input=json.dumps({"session_id": "s1"}), env=client_env(),
                         capture_output=True, text=True, timeout=60)
    assert ran.returncode == 0 and ran.stdout == "", (ran.stdout, ran.stderr)
    status = claude["statusLine"]["command"]
    ran = subprocess.run(["powershell", "-NoProfile", "-NonInteractive", "-Command", status], input="{}",
                         env=client_env(), capture_output=True, text=True, timeout=60)
    assert ran.returncode == 0, (status, ran.stdout, ran.stderr)


# ---------------------------------------------------------------- hooks --repo: right folder, kept out of git

@pytest.mark.parametrize(("remote", "key"), [
    ("https://github.com/peterrichards-lr/liferay-docker-manager.git", "github.com/peterrichards-lr/liferay-docker-manager"),
    ("git@github.com:Peterrichards-LR/Liferay-Docker-Manager.git", "github.com/peterrichards-lr/liferay-docker-manager"),
    ("ssh://git@github.com:22/owner/repo", "github.com/owner/repo"),
    ("https://www.github.com/owner/repo/", "github.com/owner/repo"),
    ("https://gitlab.example.com/group/sub/repo.git", "gitlab.example.com/sub/repo"),
])
def test_project_key_from_git_remote_matches_the_server(remote: str, key: str) -> None:
    from mcp_agent_mail.app import _repo_key_from_identifier

    assert _load_mail_wake()._project_key_of(remote) == key == _repo_key_from_identifier(remote)


def _git_repo(path: Path, origin: str | None) -> Path:
    path.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    if origin:
        subprocess.run(["git", "-C", str(path), "remote", "add", "origin", origin], check=True)
    return path


def _write_hooks(config: Path, repo: Path, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(CLIENT_SCRIPTS_DIR / "mail_wake.py"), "hooks", "claude", "--config", str(config),
         "--write", "--repo", str(repo)],
        cwd=cwd, env=client_env(), capture_output=True, text=True, timeout=60,
    )


def test_hooks_go_to_the_named_repo_and_stay_out_of_git(tmp_path: Path) -> None:
    config = tmp_path / "agent.json"
    config.write_text(json.dumps({"AGENT_MAIL_PROJECT": "github.com/owner/repo", "AGENT_MAIL_AGENT": "BlueLake"}))
    repo = _git_repo(tmp_path / "src" / "repo", "https://github.com/owner/repo.git")
    elsewhere = tmp_path / "somewhere-else"
    elsewhere.mkdir()

    done = _write_hooks(config, repo, cwd=elsewhere)  # run from another folder, as people do
    assert done.returncode == 0, done.stderr
    assert (repo / ".claude" / "settings.local.json").exists() and not (elsewhere / ".claude").exists()
    assert "warning" not in done.stdout and "kept out of git" in done.stdout
    status = subprocess.run(["git", "-C", str(repo), "status", "--short", "--untracked-files=all"],
                            capture_output=True, text=True, check=True)
    assert ".claude/settings.local.json" not in status.stdout
    again = _write_hooks(config, repo, cwd=elsewhere)
    assert "kept out of git" not in again.stdout  # already excluded: not added twice
    exclude = (repo / ".git" / "info" / "exclude").read_text()
    assert exclude.count(".claude/settings.local.json") == 1


def test_hooks_warn_when_the_folder_is_not_the_project(tmp_path: Path) -> None:
    config = tmp_path / "agent.json"
    config.write_text(json.dumps({"AGENT_MAIL_PROJECT": "github.com/owner/repo", "AGENT_MAIL_AGENT": "BlueLake"}))
    other = _git_repo(tmp_path / "other", "git@github.com:someone/else.git")
    plain = tmp_path / "plain"
    plain.mkdir()
    wrong = _write_hooks(config, other, cwd=other)
    assert wrong.returncode == 0 and "settings are for project github.com/owner/repo" in wrong.stdout
    not_git = _write_hooks(config, plain, cwd=plain)
    assert not_git.returncode == 0 and "isn't a git repository" in not_git.stdout
