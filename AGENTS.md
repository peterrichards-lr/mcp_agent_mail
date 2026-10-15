RULE NUMBER 1 (NEVER EVER EVER FORGET THIS RULE!!!): YOU ARE NEVER ALLOWED TO DELETE A FILE WITHOUT EXPRESS PERMISSION FROM ME OR A DIRECT COMMAND FROM ME. EVEN A NEW FILE THAT YOU YOURSELF CREATED, SUCH AS A TEST CODE FILE. YOU HAVE A HORRIBLE TRACK RECORD OF DELETING CRITICALLY IMPORTANT FILES OR OTHERWISE THROWING AWAY TONS OF EXPENSIVE WORK THAT I THEN NEED TO PAY TO REPRODUCE. AS A RESULT, YOU HAVE PERMANENTLY LOST ANY AND ALL RIGHTS TO DETERMINE THAT A FILE OR FOLDER SHOULD BE DELETED. YOU MUST **ALWAYS** ASK AND *RECEIVE* CLEAR, WRITTEN PERMISSION FROM ME BEFORE EVER EVEN THINKING OF DELETING A FILE OR FOLDER OF ANY KIND!!!

## Fork practices (peterrichards-lr/mcp_agent_mail)

This is an independent fork ([0006](docs/decisions/0006-independent-fork-never-pushes-upstream.md)). Its own rules come first; [CONTRIBUTING.md](CONTRIBUTING.md) has the step-by-step flow.

- **Pull requests:** every change lands on `main` through one ([0008](docs/decisions/0008-changes-land-through-pull-requests.md)). Work on a branch in its own worktree, open the PR as a draft, and mark it ready when it's done.
- **Review before ready** ([0007](docs/decisions/0007-review-before-ready.md)): before a PR is marked ready, a second agent reviews the diff for real defects. Fix every finding, or answer it in the PR. Then mark it ready; don't wait to be asked.
- **Worktrees, one per agent:** agents don't share a checkout. Each works in its own `git worktree` on its own branch (see "Worktrees" near the end of this file).
- **Decision records** live in [`docs/decisions/`](docs/decisions/README.md): why a choice was made, the owner's words quoted, what it costs, and when to revisit it. Read the relevant record before reopening one of those choices. Add a record when the owner settles something someone would reasonably reopen.
- **Issues** are on the fork. `gh` defaults to upstream here, so pass `-R peterrichards-lr/mcp_agent_mail`. Never push or open anything upstream.
- **Client scripts:** when `scripts/hooks/mail_wake.py` or `agent_mail_files.py` changes, bump `CLIENT_VERSION` in `mail_wake.py`.
- **Never weaken a gate to land a change.** Don't loosen, skip or delete a test, check or CI step to make your change pass. If a gate is genuinely wrong, fix it in its own pull request that explains why, and show what the fix lets through.

---

### IRREVERSIBLE GIT & FILESYSTEM ACTIONS — DO-NOT-EVER BREAK GLASS

1. **Absolutely forbidden commands:** `git reset --hard`, `git clean -fd`, `rm -rf`, or any command that can delete or overwrite code/data must never be run unless the user explicitly provides the exact command and states, in the same message, that they understand and want the irreversible consequences.
2. **No guessing:** If there is any uncertainty about what a command might delete or overwrite, stop immediately and ask the user for specific approval. “I think it’s safe” is never acceptable.
3. **Inspect freely, mutate carefully:** Read-only commands (`git status`, `git diff`, `git log`, `git show`, reading files) never need permission — run them whenever they help you understand the state of the tree. When cleanup or rollbacks are needed, prefer reversible options (copying to backups, committing) and ask before anything that rewrites the working tree or index. `git stash` is a mutation, not a read: it takes uncommitted work out of the tree, where it's easily forgotten or lost, so it needs the same approval as any other mutation. Only after that should a destructive command even be considered.
4. **Mandatory explicit plan:** Even after explicit user authorization, restate the command verbatim, list exactly what will be affected, and wait for a confirmation that your understanding is correct. Only then may you execute it—if anything remains ambiguous, refuse and escalate.
5. **Document the confirmation:** When running any approved destructive command, record (in the session notes / final response) the exact user text that authorized it, the command actually run, and the execution time. If that record is absent, the operation did not happen.

We only use uv in this project, NEVER pip. And we use a venv. And we ONLY target Python 3.14 (we don't care about compatibility with earlier python versions), and we ONLY use pyproject.toml (not requirements.txt) for managing the project.

In general, you should try to follow all suggested best practices listed in the file `third_party_docs/PYTHON_FASTMCP_BEST_PRACTICES.md`. That file is a general-purpose reference written for a whole family of projects; where it disagrees with this file, this file wins. Concretely: we target Python 3.14 (not 3.13), we type-check with `ty` (not mypy), and the existing `tests/` suite — pytest unit tests plus `tests/integration/` and `tests/e2e/` against a real server — is the model for new tests here, so its "no unit tests" line does not apply to this repo. Mocks never establish live behavior; if a claim is about the real service, prove it with an integration or e2e test.

You can also consult `third_party_docs/fastmcp_distilled_docs.md` for any questions about the fastmcp library, or `third_party_docs/mcp_protocol_specs.md` for any questions about the MCP protocol in general. For anything relating to Postgres, be sure to read `third_party_docs/POSTGRES18_AND_PYTHON_BEST_PRACTICES.md`.

We load all configuration details from the existing .env file (even if you can't see this file, it DOES exist, and must NEVER be overwritten!). We NEVER use os.getenv() or dotenv or other methods to get variables from our .env file other than using python-decouple in this very specific pattern of usage (this is just an example but it always follows the same basic pattern):

```
from decouple import Config as DecoupleConfig, RepositoryEnv

# Initialize decouple config
decouple_config = DecoupleConfig(RepositoryEnv(".env"))

# Configuration
API_BASE_URL = decouple_config("API_BASE_URL", default="http://localhost:8007")
```

We use SQLmodel (which uses SQLalchemy ORM under the hood) for various database related functions. Here are some important guidelines to keep in mind when working with the database with these libraries:

Do:

- Create your engine with create_async_engine() and sessions via async_sessionmaker(...), then use async with AsyncSession(...) (or async_scoped_session) so sessions are closed automatically.
- Await every database operation that’s defined as async: await session.execute(...), await session.scalars(...), await session.stream(...), await session.commit(), await session.rollback(), etc.
- Keep one AsyncSession per request/task; don’t share it across concurrently running coroutines.
- Use session.scalars(select(...)), .first(), .one(), or .unique() to get ORM entities; use session.stream(...)/stream_scalars(...) when you need server-side streaming.
- Wrap sync-only work inside await session.run_sync(...) when you must call a synchronous SQLAlchemy helper.
- Explicitly load relationships (e.g., selectinload, joinedload) or use await obj.awaitable_attrs.<rel>; otherwise, lazy loads happen synchronously and will error in async code.
- On shutdown, call await engine.dispose() to close the async engine’s pool.

Don’t:

- Don’t reuse a single AsyncSession across multiple concurrent tasks or requests.
- Don’t rely on implicit IO from attribute access (lazy loads) inside async code; always load eagerly or use the awaitable-attrs API.
- Don’t mix sync drivers or sync SQLAlchemy APIs with async sessions (e.g., avoid psycopg2 with async session—use asyncpg).
- Don’t “double-await” result helpers: methods like result.scalars().all() or result.mappings().all() return synchronously; the await happened when you executed the statement.
- Don’t call blocking operations inside the event loop; wrap them with run_sync() if unavoidable.
- Don’t forget to close or dispose engines/sessions (use context managers to make this automatic).


NEVER run a script that processes/changes code files in this repo, EVER! That sort of brittle, regex based stuff is always a huge disaster and creates far more problems than it ever solves. DO NOT BE LAZY AND ALWAYS MAKE CODE CHANGES MANUALLY, EVEN WHEN THERE ARE MANY INSTANCES TO FIX. IF THE CHANGES ARE MANY BUT SIMPLE, THEN USE SEVERAL SUBAGENTS IN PARALLEL TO MAKE THE CHANGES GO FASTER. But if the changes are subtle/complex, then you must methodically do them all yourself manually!

We do not care at all about backwards compatibility since we are still in early development with no users-- we just want to do things the RIGHT way in a clean, organized manner with NO TECH DEBT. That means, never create "compatibility shims" or any other nonsense like that.

We need to AVOID uncontrolled proliferation of code files. If you want to change something or add a feature, then you MUST revise the existing code file in place. You may NEVER, *EVER* take an existing code file, say, "document_processor.py" and then create a new file called "document_processorV2.py", or "document_processor_improved.py", or "document_processor_enhanced.py", or "document_processor_unified.py", or ANYTHING ELSE REMOTELY LIKE THAT! New code files are reserved for GENUINELY NEW FUNCTIONALITY THAT MAKES ZERO SENSE AT ALL TO INCLUDE IN ANY EXISTING CODE FILE. It should be an *INCREDIBLY* high bar for you to EVER create a new code file!

We want all console output to be informative, detailed, stylish, colorful, etc. by fully leveraging the rich library wherever possible.

If you aren't 100% sure about how to use a third party library, then you must SEARCH ONLINE to find the latest documentation website for the library to understand how it is supposed to work and the latest (mid-2025) suggested best practices and usage.

**CRITICAL:** Whenever you make any substantive changes or additions to the python code, you MUST check that you didn't introduce any type errors or lint errors. You can do this by running the following two commands:

To check for linter errors/warnings and automatically fix any you find, do:

`ruff check --fix --unsafe-fixes`

To check for type errors, do:

`uvx ty check`

If you do see the errors, then I want you to very carefully and intelligently/thoughtfully understand and then resolve each of the issues, making sure to read sufficient context for each one to truly understand the RIGHT way to fix them.

Run these from the root of your worktree, against the whole project. The goal is zero lint and type errors project-wide, so a fix pass that touches files you did not edit is fine. What is not acceptable is committing a fix pass blind: read the diff `ruff` produced so you understand every change it made before it goes into a commit. (`make lint-check typecheck` runs both without changing anything; `make help` lists the other targets.)


## MCP Agent Mail — coordination for multi-agent workflows

What it is
- A mail-like layer that lets coding agents coordinate asynchronously via MCP tools and resources.
- Provides identities, inbox/outbox, searchable threads, and advisory file reservations, with human-auditable artifacts in Git.

Why it's useful
- Prevents agents from stepping on each other with explicit file reservations (leases) for files/globs.
- Keeps communication out of your token budget by storing messages in a per-project archive.
- Offers quick reads (`resource://inbox/...`, `resource://thread/...`) and macros that bundle common flows.

How to use effectively
1) Same repository
   - Register an identity: call `ensure_project`, then `register_agent`. For `project_key`, use the repository, not a path: `github.com/peterrichards-lr/mcp_agent_mail`. A path differs in every worktree, so each worktree would become its own project, and agents wouldn't see each other's reservations.
   - Reserve files before you edit: `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)` to signal intent and avoid conflict.
   - Communicate with threads: use `send_message(..., thread_id="FEAT-123")`; check inbox with `fetch_inbox` and acknowledge with `acknowledge_message`.
   - Read fast: `resource://inbox/{Agent}?project=<abs-path>&limit=20` or `resource://thread/{id}?project=<abs-path>&include_bodies=true`.
   - Tip: set `AGENT_NAME` in your environment so the pre-commit guard can block commits that conflict with others' active exclusive file reservations.
   - Tip: worktree mode (opt-in): set `WORKTREES_ENABLED=1`, and during trials set `AGENT_MAIL_GUARD_MODE=warn`. Check hooks with `mcp-agent-mail guard status .` and identity with `mcp-agent-mail mail status .`.

2) Across different repos in one project (e.g., Next.js frontend + FastAPI backend)
   - Option A (single project bus): register both sides under the same `project_key` (shared key/path). Keep reservation patterns specific (e.g., `frontend/**` vs `backend/**`).
   - Option B (separate projects): each repo has its own `project_key`; use `macro_contact_handshake` or `request_contact`/`respond_contact` to link agents, then message directly. Keep a shared `thread_id` (e.g., ticket key) across repos for clean summaries/audits.

Macros vs granular tools
- Prefer macros when you want speed or are on a smaller model: `macro_start_session`, `macro_prepare_thread`, `macro_file_reservation_cycle`, `macro_contact_handshake`.
- Use granular tools when you need control: `register_agent`, `file_reservation_paths`, `send_message`, `fetch_inbox`, `acknowledge_message`.

### Worktree recipes (opt-in, non-disruptive)

- Enable gated features:
  - Set `WORKTREES_ENABLED=1` or `GIT_IDENTITY_ENABLED=1` in `.env` (do not commit secrets; config is loaded via `python-decouple`).
  - For trial posture, set `AGENT_MAIL_GUARD_MODE=warn` to surface conflicts without blocking.
- Inspect identity for a worktree:
  - CLI: `mcp-agent-mail mail status .`
  - Resource: `resource://identity/{/abs/path}` (available only when `WORKTREES_ENABLED=1`)
- Install guards (chain-runner friendly; honors `core.hooksPath` and Husky):
  - `mcp-agent-mail guard status .`
  - `mcp-agent-mail guard install <project_key> . --prepush`
  - Guards exit early when `WORKTREES_ENABLED=0` or `AGENT_MAIL_BYPASS=1`.
- Install the secrets pre-commit guard (one-time per clone):
  - `git config core.hooksPath .githooks`
  - `.githooks/pre-commit` rejects staged files that look like private keys
    (`*.key`, `*.priv`, `*.pem`, `id_rsa`, `signing-*` without `.pub`, etc.)
    and blob bodies that contain provider-token shapes
    (`sk-...`, `ghp_...`, `AKIA...`, PEM `-----BEGIN ... PRIVATE KEY-----`, etc.)
  - This is a second layer over `.gitignore`: the gitignore stops innocent
    `git add`, the hook stops `git add -f`. Both layers are needed because
    a real Ed25519 signing key was once committed despite `signing-*.key`
    being in `.gitignore`.
  - Bypass for confirmed false positives only: `git commit --no-verify`, saying so in the commit message. CI's gitleaks scan (`.github/workflows/secrets.yml`) still checks every pushed commit.
- Composition details:
  - Installer writes a Python chain-runner to `.git/hooks/pre-commit` and `.git/hooks/pre-push` that executes `hooks.d/<hook>/*` and then `<hook>.orig` if present.
  - Agent Mail guard is installed as `hooks.d/pre-commit/50-agent-mail.py` and `hooks.d/pre-push/50-agent-mail.py`.
  - On Windows, `.cmd` and `.ps1` shims are written alongside the chain-runner to invoke Python.
- Reserve before you edit:
  - `file_reservation_paths(project_key, agent_name, ["src/**"], ttl_seconds=3600, exclusive=true)`
  - Patterns use Git pathspec semantics and respect repository `core.ignorecase`.

### Git-based identity: precedence and migration

- Precedence (when gate is on):
  1) Committed marker `.agent-mail-project-id`
  2) Discovery YAML `.agent-mail.yaml` with `project_uid:`
  3) Private marker `.git/agent-mail/project-id`
  4) Remote fingerprint: normalized `origin` + default branch
  5) `git-common-dir` or path hash
- Migration helpers (CLI):
  - Write committed marker: `mcp-agent-mail projects mark-identity . --commit`
  - Scaffold discovery YAML: `mcp-agent-mail projects discovery-init . --product <product_uid>`

### Guard usage quickstart

- Set your identity for local commits:
  - Export `AGENT_NAME="YourAgentName"` in the shell that performs commits.
- Pre-commit:
  - Scans staged changes (`git diff --cached --name-status -M -z`) and blocks conflicts with others’ active exclusive reservations.
- Pre-push:
  - Enumerates to-be-pushed commits (`git rev-list`) and diffs trees (`git diff-tree --no-ext-diff -z`) to catch conflicts not staged locally.
- Advisory mode:
  - With `AGENT_MAIL_GUARD_MODE=warn`, conflicts are printed with rich context and push/commit proceeds.

### Build slots for long-running tasks

- Acquire a slot (advisory):
  - `acquire_build_slot(project_key, agent_name, "frontend-build", ttl_seconds=3600, exclusive=true)`
- Keep it fresh during the run:
  - `renew_build_slot(project_key, agent_name, "frontend-build", extend_seconds=1800)`
- Release when done (non-destructive; marks released):
  - `release_build_slot(project_key, agent_name, "frontend-build")`
- Tips:
  - Combine with `mcp-agent-mail amctl env --path . --agent $AGENT_NAME` to get `CACHE_KEY` and `ARTIFACT_DIR`.
  - Use `mcp-agent-mail am-run <slot> -- <cmd...>` to run with prepped env; flags include `--ttl-seconds`, `--shared/--exclusive`, and `--block-on-conflicts`. Future versions will auto-acquire/renew/release.

### Product Bus

- Create or ensure a product:
  - `mcp-agent-mail products ensure MyProduct --name "My Product"`
- Link a repo/worktree into the product (use slug or path):
  - `mcp-agent-mail products link MyProduct .`
- View product status and linked projects:
  - `mcp-agent-mail products status MyProduct`
- Search messages across all linked projects:
  - `mcp-agent-mail products search MyProduct "gh-21 OR \"release plan\"" --limit 50`
- Product-wide inbox for an agent:
  - `mcp-agent-mail products inbox MyProduct YourAgent --limit 50 --urgent-only --include-bodies`
- Product-wide thread summarization:
  - `mcp-agent-mail products summarize-thread MyProduct "gh-21" --per-thread-limit 100 --no-llm`

Server tools (for orchestrators)
- `ensure_product(product_key|name)`
- `products_link(product_key, project_key)`
- `resource://product/{key}`
- `search_messages_product(product_key, query, limit=20)`

Common pitfalls
- "from_agent not registered": always `register_agent` in the correct `project_key` first.
- "FILE_RESERVATION_CONFLICT": message the holder to coordinate, adjust patterns to disjoint paths, wait for expiry, or — only when the work is genuinely compatible with the holder's — use a non-exclusive reservation.
- Auth errors: if JWT+JWKS is enabled, include a bearer token with a `kid` that matches server JWKS; static bearer is used only when JWT is disabled.

## Tracking work: GitHub issues and Agent Mail threads

GitHub issues on the fork are the single task queue; Agent Mail carries the conversation about them.
- **Shared identifiers:** use the issue number as the Mail `thread_id` (e.g. `gh-21`) and prefix subjects with `[#21]`.
- **Reservations:** when starting an issue, call `file_reservation_paths(...)` for the paths you'll touch, with the issue in the `reason`, and release them when the pull request is merged.
- **Don't create or manage tasks in Mail.** (The `.beads/` folder is upstream's; this fork doesn't use beads, and `br`/`bv` aren't installed.)

---

## Landing the Plane (Session Completion)

**When a work session produced changes**, it isn't finished until they're in a pull request that's ready for the owner. A read-only session (a review, an investigation, answering a question) has nothing to push: file issues for anything you found, then hand off.

1. **File issues** for anything that needs follow-up (`gh issue create -R peterrichards-lr/mcp_agent_mail`).
2. **Run the checks** if code changed: `make lint-check typecheck`, plus the tests for what you touched (`make test-client` for the client and installers).
3. **Push your branch, and open or update the pull request:** `gh pr create -R peterrichards-lr/mcp_agent_mail --draft`, filling in the template's checklist.
4. **Second-agent review**, then fix or answer every finding in the PR, then `gh pr ready`. Don't leave finished work as a draft or wait to be asked to publish it.
5. **After the owner merges:** deploy if the change touches the hosted server (see the README's deployment notes), close the issue, and ask the owner whether to remove your worktree (RULE NUMBER 1 covers worktrees and branches too).
6. **Hand off:** say what's done, what's waiting on whom, and what's next.

Work isn't done while it sits only on your machine. If a push fails, resolve it and push again.

---

## Worktrees

Agents don't share a checkout here. Each agent works in its own git worktree, on its own branch:

```bash
git worktree add ../mcp_agent_mail-<topic> -b <topic> origin/main
```

- **Stay inside your worktree.** Never edit, stash, reset or check out anything in someone else's.
- **Unexpected changes in your own worktree** are yours to explain: check `git log` and `git status` before asking.
- **Shared resources aren't isolated:** the `.env`, the local database and any running server. Coordinate those through Agent Mail (file reservations, build slots), registered under the repository key `github.com/peterrichards-lr/mcp_agent_mail`, which is the same in every worktree.
- **When finished:** after your branch is merged, ask the owner before removing the worktree or deleting the branch (RULE NUMBER 1). Leave other worktrees and branches alone.

For any web requests you must make with curl or otherwise, identify yourself honestly: use a User-Agent that names this project, e.g. "mcp_agent_mail-agent (+https://github.com/Dicklesworthstone/mcp_agent_mail)". Never borrow another product's or company's crawler or client User-Agent, and never spoof a client to get past a block.
