STATUS: Accepted 2026-10-15

# 0007. A second agent reviews every change before its pull request is marked ready

## Decision

Before a feature or fix lands, a second agent reviews the diff for real defects, meaning bugs that passing
tests don't show, not style. Its findings are fixed, or answered in the pull request, before the pull request
is marked ready.

Owner, 2026-10-15, approving issue #15, which proposed this: "Ok go with 9 and 12, followed by 15". The idea
comes from lfr-tunnel's decision 0005. There, a reviewer found four real bugs on green CI within a day.

**The mechanism was at first chosen by the session, not the owner.** At first this fork committed straight to
`main`, so the review was a step before pushing. On 2026-10-15 the owner switched to pull requests
([0008](0008-changes-land-through-pull-requests.md)), so the lfr-tunnel mechanism now applies: open the pull
request as a draft, and mark it ready once the review's findings are dealt with. Publishing is part of the work:
a reviewed pull request is marked ready without waiting to be asked.

First run (2026-10-15, reviewing #9 and #12 after they had shipped with all tests passing): six real defects.
Two were serious: Auth0-style issuers rejected every sign-in, and personal access tokens outlived SSO-only
mode and allowlist removal. All were fixed in the same day's follow-up commit.

## Cost

- A second agent and a second round on every change, including small ones.
- Reviews of security-sensitive code, such as sign-in and the client scripts, take longest, and that's where they're most worth it.

## Enforced by

`unenforced by design`: no gate can tell whether a review happened or whether it was any good. The rule is
stated in `AGENTS.md` ("Fork practices").

## Revisit when

The reviewer stops finding real defects across a meaningful run of changes. At that point the cost is real
and the return isn't.
