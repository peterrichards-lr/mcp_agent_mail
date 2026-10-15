# Security policy

This policy covers [peterrichards-lr/mcp_agent_mail](https://github.com/peterrichards-lr/mcp_agent_mail), an independent fork of [Dicklesworthstone/mcp_agent_mail](https://github.com/Dicklesworthstone/mcp_agent_mail). Report problems in upstream's code to upstream.

## Reporting a vulnerability

Report it privately through GitHub: **Security → Report a vulnerability** on this repository, which creates a private advisory. Please don't open a public issue or pull request for it. Include what's affected, how to reproduce it, and what an attacker could do with it.

Expect an acknowledgement within a few days. Fixes go to `main` and to the hosted server first; the advisory is published after that.

## What's in scope

This fork runs as an internet-facing server that agents on other machines connect to. The parts that matter most:

- **The HTTP server** (`src/mcp_agent_mail/http.py`): the MCP endpoint, `/mail/api/*`, and the web UI.
- **Authentication:**
  - the server's bearer token;
  - personal access tokens (`amt_…`);
  - web sessions;
  - GitHub, password + TOTP MFA, and OpenID Connect sign-in (`ui_auth.py`, `oidc.py`);
  - per-IP throttling and bans (`ip_guard.py`).
- **Authorization between agents:** registration tokens, contact policies, and project scoping.
- **The agent-machine client:**
  - `scripts/hooks/mail_wake.py` and `agent_mail_files.py`, which run on people's machines and are served at `/client/`;
  - the installers `/install.sh` and `/install.ps1`;
  - `mail_wake.py update`, which replaces code on those machines. Signature verification is tracked in #17.
- **Data at rest:** the SQLite database, the Git mailbox, the attachment blob store, and backups.

Out of scope: denial of service by volume, findings that need an already-compromised server or agent machine, and anything in upstream code that this fork doesn't run.

## Secrets

- **Configuration:** comes from `.env` (python-decouple) and is never committed. Client settings live outside the repository (`~/.config/agent-mail/*.json`, readable only by their owner).
- **Two layers stop secrets being committed:**
  - `.githooks/pre-commit`, enabled per clone with `git config core.hooksPath .githooks`, which rejects key files and token-shaped strings;
  - gitleaks in CI (`.github/workflows/secrets.yml`), which scans every pushed commit even where the hook isn't enabled.
- **Old leaks:** upstream once committed a signing key, and a bearer token in `.claude/settings.json`. Neither is valid for this fork's server. The token is gone from the current tree, which now holds only permissions, but remains in history.
- **If a secret leaks:** rotate it. For the server token, see the README's deployment notes. Personal access tokens can be revoked at `/mail/account/tokens`.

## Supply chain

- GitHub Actions are pinned to full commit SHAs, and Dependabot proposes updates after a cooldown.
- Python dependencies are locked in `uv.lock`, and the Docker image builds from it (`--frozen`).
- The web UI still loads front-end libraries from public CDNs, some of them unpinned. Moving them to pinned, self-hosted copies with a Content-Security-Policy is tracked in #20.
