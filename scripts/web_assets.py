"""Build and check the web UI's static files: its CSS and its vendored third-party files (#20).

    python scripts/web_assets.py vendor   # download frontend/vendor.json's files, checking each hash
    python scripts/web_assets.py css      # build static/app.css with the pinned Tailwind binary
    python scripts/web_assets.py check    # both are current: vendored files match, CSS rebuilds the same

The pages used to load about 27 files from four CDNs, and Tailwind's play CDN compiled the CSS in
every browser on every page. Everything is now served from /static/, built and pinned here.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import platform
import stat
import subprocess
import sys
import tempfile
import urllib.request
from pathlib import Path

from rich.console import Console

REPO_ROOT = Path(__file__).resolve().parent.parent
FRONTEND = REPO_ROOT / "frontend"
MANIFEST = FRONTEND / "vendor.json"
STATIC = REPO_ROOT / "src" / "mcp_agent_mail" / "static"
VENDOR_DIR = STATIC / "vendor"
CSS_OUT = STATIC / "app.css"
CACHE_DIR = Path.home() / ".cache" / "mcp_agent_mail"
USER_AGENT = "mcp_agent_mail-agent (+https://github.com/Dicklesworthstone/mcp_agent_mail)"

console = Console()


def _manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def _fetch(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def integrity(data: bytes) -> str:
    """The Subresource Integrity value of `data`, as the manifest records it."""
    return "sha384-" + base64.b64encode(hashlib.sha384(data).digest()).decode()


def vendor() -> int:
    files = _manifest()["files"]
    VENDOR_DIR.mkdir(parents=True, exist_ok=True)
    for name, entry in files.items():
        data = _fetch(entry["url"])
        if integrity(data) != entry["integrity"]:
            console.print(f"[bold red]✗ {name}[/]: {entry['url']} doesn't match its integrity hash; nothing written")
            return 1
        (VENDOR_DIR / name).write_bytes(data)
        console.print(f"[green]✓[/] {name} [dim]{len(data):,} bytes[/]")
    stale = sorted(p.name for p in VENDOR_DIR.iterdir() if p.name not in files)
    if stale:
        console.print(f"[yellow]Not in the manifest any more, remove them:[/] {', '.join(stale)}")
    return 0


def _tailwind_platform() -> str:
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "x64"
    if sys.platform == "darwin":
        return f"macos-{arch}"
    if sys.platform == "win32":
        return f"windows-{arch}.exe"
    return f"linux-{arch}"


def _tailwind() -> Path:
    """The pinned Tailwind standalone binary for this machine, downloaded once and checked."""
    pinned = _manifest()["tailwind"]
    name = _tailwind_platform()
    expected = pinned["sha256"].get(name)
    if expected is None:
        raise SystemExit(f"No pinned Tailwind binary for {name}: add its sha256 to {MANIFEST}")
    binary = CACHE_DIR / f"tailwindcss-{pinned['version']}-{name}"
    if not binary.exists() or hashlib.sha256(binary.read_bytes()).hexdigest() != expected:
        url = pinned["url"].format(platform=name)
        console.print(f"[cyan]Downloading Tailwind {pinned['version']}[/] [dim]{url}[/]")
        data = _fetch(url)
        if hashlib.sha256(data).hexdigest() != expected:
            raise SystemExit(f"{url} doesn't match its pinned sha256")
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        binary.write_bytes(data)
        binary.chmod(binary.stat().st_mode | stat.S_IXUSR)
    return binary


def _build_css(output: Path) -> None:
    subprocess.run(
        [str(_tailwind()), "--config", str(FRONTEND / "tailwind.config.js"), "--input", str(FRONTEND / "tailwind.css"),
         "--output", str(output), "--minify"],
        cwd=REPO_ROOT, check=True, capture_output=True,
    )


def css() -> int:
    _build_css(CSS_OUT)
    console.print(f"[green]✓[/] {CSS_OUT.relative_to(REPO_ROOT)} [dim]{CSS_OUT.stat().st_size:,} bytes[/]")
    return 0


def check() -> int:
    problems = []
    files = _manifest()["files"]
    for name, entry in files.items():
        path = VENDOR_DIR / name
        if not path.exists():
            problems.append(f"{name} is missing: run `python scripts/web_assets.py vendor`")
        elif integrity(path.read_bytes()) != entry["integrity"]:
            problems.append(f"{name} doesn't match its integrity hash")
    with tempfile.TemporaryDirectory() as tmp:
        rebuilt = Path(tmp) / "app.css"
        _build_css(rebuilt)
        if not CSS_OUT.exists() or rebuilt.read_bytes() != CSS_OUT.read_bytes():
            problems.append("static/app.css is out of date with the templates: run `python scripts/web_assets.py css`")
    for problem in problems:
        console.print(f"[bold red]✗[/] {problem}")
    if not problems:
        console.print(f"[green]✓[/] {len(files)} vendored files match their hashes, and app.css is current")
    return 1 if problems else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("vendor", "css", "check"))
    command = parser.parse_args().command
    return {"vendor": vendor, "css": css, "check": check}[command]()


if __name__ == "__main__":
    sys.exit(main())
