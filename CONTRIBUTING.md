# Contributing

How work happens on [peterrichards-lr/mcp_agent_mail](https://github.com/peterrichards-lr/mcp_agent_mail), whether you're a person or an agent. The detailed rules for agents are in [AGENTS.md](AGENTS.md); this file is the short version of the flow.

## Setup

- [uv](https://docs.astral.sh/uv/) and Python 3.14, using `uv` only (never pip). Configuration comes from `.env` through python-decouple; never commit it.
- Turn on the secrets hook once per clone: `git config core.hooksPath .githooks`.
- Run `make help` to see the targets.
- **One worktree per agent:** agents don't share a checkout. Start each piece of work in its own worktree and branch: `git worktree add ../mcp_agent_mail-<topic> -b <topic> origin/main`.

## The flow

1. **An issue first.** Track work in this fork's [issues](https://github.com/peterrichards-lr/mcp_agent_mail/issues). In this clone, `gh` defaults to upstream, so pass `-R peterrichards-lr/mcp_agent_mail`. Never open issues or pull requests upstream ([decision 0006](docs/decisions/0006-independent-fork-never-pushes-upstream.md)).
2. **Build it with tests.** Claims about the live service need an integration or end-to-end test (`tests/live_server.py`); mocks don't count.
3. **Check it:**
   - Run `make lint-check typecheck` and the tests for what you touched.
   - Run `make test-client` if you changed `scripts/hooks/` or the installers. Client changes also run on Windows in CI, on the pull request and again after merging.
   - `make verify` runs everything. The full suite still has inherited failures (#24), so compare against `main` rather than expecting it to be clean.
4. **Second-agent review before it lands** ([decision 0007](docs/decisions/0007-review-before-ready.md)). A second agent reviews the diff for real defects, and every finding gets fixed or answered before the pull request is marked ready.
5. **Commit:**
   - Use conventional commit messages (`feat:`, `fix:`, `docs:`…) that name the issue (`(#12)`, or `Closes #12`).
   - Add a line under **Unreleased** in [CHANGELOG.md](CHANGELOG.md).
   - Changed `scripts/hooks/mail_wake.py` or `agent_mail_files.py`? Bump `CLIENT_VERSION` in `mail_wake.py`.
6. **Pull request** ([decision 0008](docs/decisions/0008-changes-land-through-pull-requests.md)):
   - Push your branch and open a draft pull request: `gh pr create -R peterrichards-lr/mcp_agent_mail --draft`. Fill in the template.
   - After the review, mark it ready (`gh pr ready`). The owner merges; nothing goes to `main` directly.
   - If it changes the hosted server, deploy after the merge (see the README's deployment notes).

## Decisions

When the owner settles a choice that someone would reasonably reopen, add a record to [docs/decisions/](docs/decisions/README.md). Quote the owner's own words, and say when a mechanism was chosen without them.

## Never

- Delete files, or run `git reset --hard`, `git clean`, `git stash` or `rm -rf`, without the owner's explicit go-ahead.
- Change code with scripts or regex passes. Make edits by hand.
- Commit secrets or tokens, or print them in logs and output ([SECURITY.md](SECURITY.md)).
- Bypass the secrets hook with `--no-verify` for anything but a confirmed false positive, and say so in the commit message.
