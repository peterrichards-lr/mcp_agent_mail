STATUS: Accepted 2026-10-15

# 0006. This is an independent fork and never pushes upstream

## Decision

Work is pushed to `peterrichards-lr/mcp_agent_mail` and tracked in its issues. Nothing goes to
`Dicklesworthstone/mcp_agent_mail`. Upstream fixes are pulled in selectively. The README says the fork is
independent and credits the original author.

Owner, 2026-10-15. Asked where commits should go, the owner answered: **"Yes my fork and I have enabled issues for contributors"**. Then:

> "Is the Rust version better [...] If this repo is no longer maintained then perhaps we need to remove the
> upstream because we cannot push back into it with PRs"

> "Ok, let's not switch. I agree with making the fork independence explicit."

> "I thnk even though we are no positioning this as an indepentdent repo, we should give a nod to the
> original author and repo"

## Cost

- Upstream fixes don't arrive on their own: someone has to look for them and port them.
- Fork-only features (the event stream, push delivery, presence, attachments and sign-in) would need porting if the project ever moved to the Rust server (#16).

## Enforced by

- Local git config: the `upstream` remote's push URL is `DISABLED-no-pushes-upstream`. This applies per clone and isn't committed.
- `README.md`: the independent-fork banner and the Credits section.
- `gh` defaults to the upstream repo in this clone, so pass `-R peterrichards-lr/mcp_agent_mail` for issues.

## Revisit when

Upstream becomes active again and accepts PRs, or the Rust evaluation (#16) recommends switching.
