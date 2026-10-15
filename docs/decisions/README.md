# Decisions

A decision record answers **why a choice was made, what it cost, and when to revisit it**. It covers choices
that someone without the history would reasonably reopen. The format and the idea come from
[lfr-tunnel](https://github.com/peterrichards-lr/lfr-tunnel) (`.agents/decisions/`).

These are this fork's records. [`docs/adr/`](../adr/) holds the records inherited from upstream.

- **Rules belong elsewhere.** What an agent must or must not do belongs in `AGENTS.md`, the README or the code. A record links to the rule rather than restating it.
- **Work in progress belongs elsewhere.** Open issues, branches and things being built are tracked in [GitHub issues](https://github.com/peterrichards-lr/mcp_agent_mail/issues).
- **Quote the owner's own words** wherever they exist, verbatim, with the date. When a session chose a mechanism the owner didn't specify, say so.

## Format

Use one decision per file, named `NNNN-kebab-slug.md`, and keep each under about 40 lines.

```markdown
STATUS: Accepted YYYY-MM-DD

# NNNN. Title as a statement, not a question

## Decision
What was decided, with the owner's words quoted.

## Cost
What this trades away: the price actually being paid.

## Enforced by
The code, test, config key or rule that makes this real, by path. Or the literal string
`unenforced by design`, which is a legitimate answer.

## Revisit when
The condition that reopens this.
```

## Index

| # | Decision | Status |
|---|---|---|
| [0001](0001-token-limits-are-enforced-by-agents.md) | Token limits are enforced on the agent's machine, never by the server | Accepted 2026-10-15 |
| [0002](0002-pool-scope-is-chosen-per-agent.md) | Capacity-pool scope is chosen per agent at registration, defaulting to the project | Accepted 2026-10-15, not built (#5) |
| [0003](0003-attachments-live-outside-the-git-mailbox.md) | File attachments live in a blob store, outside the Git mailbox | Accepted 2026-10-15 |
| [0004](0004-presence-never-reads-prompts.md) | Presence never takes anything from prompts | Accepted 2026-10-15 |
| [0005](0005-browser-sign-in-methods.md) | Browser sign-in: GitHub, password with mandatory MFA, company SSO; the token is a fallback | Accepted 2026-10-15 |
| [0006](0006-independent-fork-never-pushes-upstream.md) | This is an independent fork and never pushes upstream | Accepted 2026-10-15 |
| [0007](0007-review-before-ready.md) | A second agent reviews every change before it is pushed or marked ready | Accepted 2026-10-15 |
