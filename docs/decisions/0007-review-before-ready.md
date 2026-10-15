STATUS: Accepted 2026-10-15

# 0007. A second agent reviews every change before it is pushed or marked ready

## Decision

Before a feature or fix lands, a second agent reviews the diff for real defects, meaning bugs that passing
tests don't show, not style. Its findings are fixed, or answered in the commit or PR, before the change is
pushed to `main` or a PR is marked ready.

Owner, 2026-10-15, approving issue #15, which proposed this: "Ok go with 9 and 12, followed by 15". The idea
comes from lfr-tunnel's decision 0005. There, a reviewer found four real bugs on green CI within a day.

**The mechanism was not specified by the owner.** This fork has so far committed straight to `main` rather
than through PRs, so the session made the review a step before pushing. Where work goes through a PR, the
lfr-tunnel mechanism applies: open it as a draft, and mark it ready once the findings are dealt with.
Publishing is part of the work: a reviewed change is pushed or marked ready without waiting to be asked.

## Cost

- A second agent and a second round on every change, including small ones.
- Reviews of security-sensitive code, such as sign-in and the client scripts, take longest, and that's where they're most worth it.

## Enforced by

`unenforced by design`: no gate can tell whether a review happened or whether it was any good. The rule is
stated in `AGENTS.md` ("Fork practices").

## Revisit when

The reviewer stops finding real defects across a meaningful run of changes. At that point the cost is real
and the return isn't.
