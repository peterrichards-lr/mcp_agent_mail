## What and why

<!-- One or two sentences. Link the issue: Closes #… -->

## Checklist

- [ ] An issue on this fork (`-R peterrichards-lr/mcp_agent_mail`), linked above
- [ ] Tests for the change; live behaviour is proven against a real server, not mocks
- [ ] `make lint-check typecheck` clean, plus the tests for what changed (`make test-client` for client or installer changes)
- [ ] Second-agent review done, and every finding fixed or answered here ([0007](https://github.com/peterrichards-lr/mcp_agent_mail/blob/main/docs/decisions/0007-review-before-ready.md))
- [ ] CHANGELOG.md **Unreleased** updated
- [ ] `CLIENT_VERSION` bumped if `scripts/hooks/` changed
- [ ] A decision record if this settles a choice someone would reopen
- [ ] No secrets in the diff, logs or output
