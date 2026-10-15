STATUS: Accepted 2026-10-15

# 0008. Changes land on main through pull requests, each from its own worktree

## Decision

Every change reaches `main` through a pull request on the fork. An agent works on its own branch in its own git
worktree, opens the pull request as a draft, has a second agent review it ([0007](0007-review-before-ready.md)), and
marks it ready. The owner merges.

Owner, 2026-10-15, asked "Keep pushing straight to main, or switch to pull requests?":

> "I think we should switch to PRs"

and asked "Do agents really share one checkout?":

> "no, they use worktrees"

This replaces upstream's rule, from AGENTS.md, that many agents share one checkout and commit each other's
in-progress changes as their own. It also replaces this fork's earlier practice of pushing reviewed commits
straight to `main`.

## Cost

- Every change waits for the owner to merge it, including small ones and urgent server fixes.
- More branches and worktrees to keep tidy.
- CI (`ci.yml`) runs on every pull request, but it's red (#24) until inherited test failures are sorted out, so it
  can't be a required check yet. The checks that mean something on a pull request until then:
  - secret scan, on every pull request;
  - Windows client, on pull requests that touch the client or installers.
- Ending a worktree or branch after its merge still needs the owner's OK. RULE NUMBER 1 in AGENTS.md covers
  deletions, and this decision doesn't make an exception.

## Enforced by

- [AGENTS.md](../../AGENTS.md) ("Fork practices", "Landing the Plane", "Worktrees"),
  [CONTRIBUTING.md](../../CONTRIBUTING.md) and the pull request template.
- Not yet enforced by GitHub. A ruleset requiring pull requests on `main` would make it real. A solo owner can't
  approve their own pull requests, so the ruleset would require a pull request but no approving review. That's
  a setting for the owner to turn on.

## Revisit when

More than one person merges, which would make required approving reviews possible, or the wait for merges blocks
urgent server fixes often enough to need an exception path.
