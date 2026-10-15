#!/usr/bin/env python3
"""Move files between agents on different machines through Agent Mail.

Agents run this instead of passing file contents through a model: upload a
file, then put the printed id in send_message / reply_message
`attachment_ids`; the recipient downloads it by id. Downloads are checked
against the server's SHA-256, so the file arrives byte for byte. Uses the
same --config file as mail_wake.py (the token never appears in a command).

  python3 agent_mail_files.py upload REPORT.txt --config ~/.config/agent-mail/repo.json
      -> {"id": "att_...", "name": "REPORT.txt", "size": 12345, "sha256": "...", ...}
  python3 agent_mail_files.py download att_... -o references/results/ --config ...
      -> references/results/REPORT.txt  sha256 ... (verified)

download refuses to overwrite an existing file unless --force.
Standard library only.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mail_wake import USER_AGENT, Config  # same folder; shares the settings loading

CHUNK = 1024 * 1024


def _headers(cfg: Config) -> dict[str, str]:
    return {"User-Agent": USER_AGENT} | ({"Authorization": f"Bearer {cfg.token}"} if cfg.token else {})


def _fail(message: str) -> int:
    print(f"error: {message}", file=sys.stderr)
    return 1


def upload(cfg: Config, path: Path) -> int:
    if not path.is_file():
        return _fail(f"{path} is not a file")
    query = urllib.parse.urlencode({"project": cfg.project, "agent": cfg.agent, "filename": path.name})
    with path.open("rb") as fh:
        request = urllib.request.Request(
            f"{cfg.server}/mail/api/attachments?{query}",
            data=fh,  # streamed, not read into memory
            method="POST",
            headers=_headers(cfg)
            | {"Content-Type": "application/octet-stream", "Content-Length": str(path.stat().st_size)},
        )
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                meta = json.load(response)
        except urllib.error.HTTPError as exc:
            return _fail(f"upload refused ({exc.code}): {exc.read().decode(errors='replace')}")
        except (urllib.error.URLError, OSError) as exc:
            return _fail(f"upload failed: {exc}")
    print(json.dumps(meta))
    return 0


def download(cfg: Config, attachment_id: str, target: Path | None, force: bool) -> int:
    request = urllib.request.Request(
        f"{cfg.server}/mail/api/attachments/{urllib.parse.quote(attachment_id)}", headers=_headers(cfg)
    )
    try:
        response = urllib.request.urlopen(request, timeout=300)
    except urllib.error.HTTPError as exc:
        return _fail(f"download refused ({exc.code}): {exc.read().decode(errors='replace')}")
    except (urllib.error.URLError, OSError) as exc:
        return _fail(f"download failed: {exc}")
    with response:
        expected = response.headers.get("X-Content-SHA256", "")
        name = Path(response.headers.get_filename() or attachment_id).name or attachment_id
        if target is None:
            dest = Path.cwd() / name
        elif target.is_dir() or str(target).endswith(("/", "\\")):
            dest = target / name
        else:
            dest = target
        if dest.exists() and not force:
            return _fail(f"{dest} already exists (use --force to overwrite)")
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_name(f".{dest.name}.part")
        digest = hashlib.sha256()
        try:
            with tmp.open("wb") as fh:
                while chunk := response.read(CHUNK):
                    digest.update(chunk)
                    fh.write(chunk)
            if not expected or digest.hexdigest() != expected:
                return _fail(f"checksum mismatch for {attachment_id}: got {digest.hexdigest()}, expected {expected or 'none'}")
            tmp.replace(dest)
        finally:
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)
    print(f"{dest}  sha256 {expected} (verified)")
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Upload and download Agent Mail file attachments.")
    parser.add_argument("--config", type=Path, help="settings JSON (same file as mail_wake.py)")
    commands = parser.add_subparsers(dest="command", required=True)
    up = commands.add_parser("upload", help="upload a file; prints its attachment id")
    up.add_argument("file", type=Path)
    up.add_argument("--config", type=Path, help=argparse.SUPPRESS)
    down = commands.add_parser("download", help="download an attachment by id, verifying its SHA-256")
    down.add_argument("id")
    down.add_argument("-o", "--output", type=Path, help="file or directory (default: current directory)")
    down.add_argument("--force", action="store_true", help="overwrite an existing file")
    down.add_argument("--config", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv[1:])

    cfg = Config(args.config)
    if not (cfg.server and cfg.project and cfg.agent):
        return _fail("not configured: set AGENT_MAIL_SERVER_URL, AGENT_MAIL_PROJECT and AGENT_MAIL_AGENT (e.g. via --config)")
    if args.command == "upload":
        return upload(cfg, args.file)
    return download(cfg, args.id, args.output, args.force)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
