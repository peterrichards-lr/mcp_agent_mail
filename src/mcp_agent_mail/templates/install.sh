#!/bin/sh
# Agent Mail: set up an agent on this machine (push delivery, presence, attachments).
#
#   curl -fsSL https://<server>/install.sh | sh -s -- --project github.com/owner/repo --agent BlueLake
#
# It downloads the client scripts from the server that served it, signs you in through the
# browser to get a personal access token for this machine (no token is pasted or shown),
# writes the settings file outside your workspace (mode 600), and prints, or with
# --write-hooks merges into the project folder (--repo, default: here), the hook settings for Claude Code or
# Antigravity. Needs python3 and curl. macOS, Linux and WSL; on native Windows use
# /install.ps1 instead.
#
# Options:
#   --project KEY        project key, e.g. github.com/owner/repo            (required)
#   --agent NAME         this agent's name on the server                     (required)
#   --name LABEL         token name shown on the server (default: host · project)
#   --no-browser         print the sign-in URL and paste the token (e.g. over SSH)
#   --write-hooks WHICH  merge hook settings into the project for claude or antigravity
#   --repo DIR           the project folder the agent works in (default: the current folder)
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

PROJECT=""; AGENT=""; NAME=""; NO_BROWSER=""; WRITE_HOOKS=""; REPO="."
while [ $# -gt 0 ]; do
  case "$1" in
    --project) PROJECT="${2:-}"; shift 2 ;;
    --agent) AGENT="${2:-}"; shift 2 ;;
    --name) NAME="${2:-}"; shift 2 ;;
    --no-browser) NO_BROWSER="--no-browser"; shift ;;
    --write-hooks) WRITE_HOOKS="${2:-}"; shift 2 ;;
    --repo) REPO="${2:-}"; shift 2 ;;
    -h | --help) sed -n '2,20p' "$0" 2>/dev/null || true; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 2 ;;
  esac
done
if [ -z "$PROJECT" ] || [ -z "$AGENT" ]; then
  echo "Usage: sh install.sh --project github.com/owner/repo --agent NAME [--no-browser] [--write-hooks claude|antigravity] [--repo DIR]" >&2
  exit 2
fi
case "$WRITE_HOOKS" in "" | claude | antigravity) ;; *) echo "--write-hooks takes claude or antigravity" >&2; exit 2 ;; esac
[ -d "$REPO" ] || { echo "--repo: $REPO isn't a folder" >&2; exit 2; }
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

# mail_wake.py writes the hook settings itself (the same code serves install.ps1), merging
# into the repo's settings and replacing any earlier Agent Mail hooks rather than duplicating them.
case "$WRITE_HOOKS" in
  claude) echo "==> Claude Code hooks"; python3 "$HOME_DIR/mail_wake.py" hooks claude --config "$CONFIG" --write --repo "$REPO" ;;
  antigravity) echo "==> Antigravity hooks"; python3 "$HOME_DIR/mail_wake.py" hooks antigravity --config "$CONFIG" --write --repo "$REPO" ;;
  *)
    echo "==> Hook settings (or rerun with --write-hooks claude|antigravity --repo <project folder>)"
    echo "--- Claude Code: .claude/settings.local.json"; python3 "$HOME_DIR/mail_wake.py" hooks claude --config "$CONFIG"
    echo "--- Antigravity: .agents/hooks.json"; python3 "$HOME_DIR/mail_wake.py" hooks antigravity --config "$CONFIG" ;;
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
