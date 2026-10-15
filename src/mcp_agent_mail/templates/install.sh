#!/bin/sh
# Agent Mail: set up an agent on this machine (push delivery, presence, attachments).
#
#   curl -fsSL https://<server>/install.sh | sh -s -- --project github.com/owner/repo --agent BlueLake
#
# It downloads the client scripts from the server that served it, signs you in through the
# browser to get a personal access token for this machine (no token is pasted or shown),
# writes the settings file outside your workspace (mode 600), and prints, or with
# --write-hooks merges into the current directory, the hook settings for Claude Code or
# Antigravity. Needs python3 and curl. macOS/Linux (WSL counts); native Windows isn't
# supported yet.
#
# Options:
#   --project KEY        project key, e.g. github.com/owner/repo            (required)
#   --agent NAME         this agent's name on the server                     (required)
#   --name LABEL         token name shown on the server (default: host · project)
#   --no-browser         print the sign-in URL and paste the token (e.g. over SSH)
#   --write-hooks WHICH  merge hook settings into ./ for claude or antigravity
set -eu

# Filled in by the server that serves this file. The check below matches the placeholder
# without its trailing underscores, so serving can't rewrite the check along with it
# (lfr-tunnel #1684 made exactly that mistake).
SERVER="__AGENT_MAIL_SERVER__"
case "$SERVER" in
  "" | *__AGENT_MAIL_SERVER*)
    echo "This installer doesn't know its server. Fetch it from your server instead:" >&2
    echo "  curl -fsSL https://<your-agent-mail-server>/install.sh | sh -s -- --project ... --agent ..." >&2
    exit 1 ;;
esac

PROJECT=""; AGENT=""; NAME=""; NO_BROWSER=""; WRITE_HOOKS=""
while [ $# -gt 0 ]; do
  case "$1" in
    --project) PROJECT="${2:-}"; shift 2 ;;
    --agent) AGENT="${2:-}"; shift 2 ;;
    --name) NAME="${2:-}"; shift 2 ;;
    --no-browser) NO_BROWSER="--no-browser"; shift ;;
    --write-hooks) WRITE_HOOKS="${2:-}"; shift 2 ;;
    -h | --help) sed -n '2,20p' "$0" 2>/dev/null || true; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$PROJECT" ] || [ -z "$AGENT" ]; then
  echo "Usage: sh install.sh --project github.com/owner/repo --agent NAME [--no-browser] [--write-hooks claude|antigravity]" >&2
  exit 2
fi
case "$WRITE_HOOKS" in "" | claude | antigravity) ;; *) echo "--write-hooks takes claude or antigravity" >&2; exit 2 ;; esac
command -v python3 >/dev/null 2>&1 || { echo "python3 is required." >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl is required." >&2; exit 1; }

HOME_DIR="${AGENT_MAIL_HOME:-$HOME/.agent-mail}"
CONFIG_DIR="${AGENT_MAIL_CONFIG_DIR:-$HOME/.config/agent-mail}"
CONFIG="$CONFIG_DIR/$(printf '%s__%s' "$PROJECT" "$AGENT" | tr -c 'A-Za-z0-9._-' '_').json"
mkdir -p "$HOME_DIR" "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"

echo "==> Downloading client scripts from $SERVER to $HOME_DIR"
for script in mail_wake.py agent_mail_files.py; do
  curl -fsSL -A "mcp_agent_mail-installer" "$SERVER/client/$script" -o "$HOME_DIR/$script.part"
  mv "$HOME_DIR/$script.part" "$HOME_DIR/$script"
done

echo "==> Signing in to get an access token for this machine"
set -- --config "$CONFIG" --server "$SERVER" --project "$PROJECT" --agent "$AGENT"
[ -n "$NAME" ] && set -- "$@" --name "$NAME"
[ -n "$NO_BROWSER" ] && set -- "$@" --no-browser
# stdin may be the curl pipe; --no-browser needs the terminal for the pasted token.
if [ -n "$NO_BROWSER" ] && [ -r /dev/tty ]; then
  python3 "$HOME_DIR/mail_wake.py" login "$@" < /dev/tty
else
  python3 "$HOME_DIR/mail_wake.py" login "$@"
fi

WAKE="python3 $HOME_DIR/mail_wake.py"
C="--config $CONFIG"
CLAUDE_HOOKS=$(cat <<EOF
{
  "statusLine": {"type": "command", "command": "$WAKE record-usage $C"},
  "hooks": {
    "Stop": [{"hooks": [{"type": "command", "asyncRewake": true, "timeout": 3600, "command": "$WAKE wait $C"}]}],
    "SessionStart": [{"hooks": [{"type": "command", "command": "$WAKE context $C"}]}],
    "UserPromptSubmit": [{"hooks": [{"type": "command", "command": "$WAKE context $C"}]}]
  }
}
EOF
)
AGY_HOOKS=$(cat <<EOF
{"agent-mail": {"enabled": true,
  "SessionStart":  [{"type": "command", "timeout": 10, "command": "$WAKE agy-context $C"}],
  "PreInvocation": [{"type": "command", "timeout": 10, "command": "$WAKE agy-context $C"}],
  "Stop":          [{"type": "command", "timeout": 10, "command": "$WAKE agy-stop $C"}]}}
EOF
)

merge() {  # merge JSON $2 into file $1, keeping everything already there
  python3 - "$1" "$2" <<'PY'
import json, sys
from pathlib import Path
path, new = Path(sys.argv[1]), json.loads(sys.argv[2])
current = json.loads(path.read_text()) if path.exists() and path.read_text().strip() else {}
def deep(a, b):
    for k, v in b.items():
        a[k] = deep(a.get(k, {}), v) if isinstance(v, dict) and isinstance(a.get(k), dict) else v
    return a
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(deep(current, new), indent=2) + "\n")
print(f"    merged into {path}")
PY
}

case "$WRITE_HOOKS" in
  claude) echo "==> Claude Code hooks"; merge ".claude/settings.local.json" "$CLAUDE_HOOKS" ;;
  antigravity) echo "==> Antigravity hooks"; merge ".agents/hooks.json" "$AGY_HOOKS" ;;
  *)
    echo "==> Hook settings (or rerun with --write-hooks claude|antigravity inside the repo)"
    echo "--- Claude Code: .claude/settings.local.json"; echo "$CLAUDE_HOOKS"
    echo "--- Antigravity: .agents/hooks.json"; echo "$AGY_HOOKS" ;;
esac

cat <<EOF

==> Connect the agent to the server's MCP endpoint (the token is read from the settings file,
    so it never lands in your shell history):
    Claude Code:  claude mcp add --transport http agent-mail $SERVER/mcp/ \\
                    --header "Authorization: Bearer \$(python3 -c 'import json;print(json.load(open("$CONFIG"))["AGENT_MAIL_TOKEN"])')"
    Antigravity:  agy mcp add --header "Authorization: Bearer \$(python3 -c 'import json;print(json.load(open("$CONFIG"))["AGENT_MAIL_TOKEN"])')" \\
                    agent-mail $SERVER/mcp/
Done. Settings: $CONFIG
EOF
