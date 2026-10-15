STATUS: Accepted 2026-10-15

# 0004. Presence never takes anything from prompts

## Decision

Presence shows whether an agent is busy, idle or offline, and whether it can be woken. It comes only from the
hooks' timing and from the agent's own MCP calls. The "working on" note is whatever the agent chooses to say
with `set_status`; nothing is taken from the user's prompts or the agent's output. The machine name is the
hostname, which the owner can override.

Owner's answers, 2026-10-15:
- To "Should the 'working on' note be taken from what the agent is doing automatically?": **"Agent sets it (Recommended)"**.
- To "What machine name should agents report in presence (visible to anyone with the server token)?": **"Hostname, overridable (Recommended)"**.

## Cost

- The "working on" note is often empty or stale, because agents forget to set it.
- People reading the Live page get less context than an automatic summary would give.

## Enforced by

- `scripts/hooks/mail_wake.py`: `_presence` sends project, agent, state, listener, machine, client, session and version, and nothing from the hook input's prompt.
- `src/mcp_agent_mail/app.py`: the `set_status` tool.
- `AGENT_MAIL_MACHINE` overrides the hostname.
- `tests/test_presence.py`.

## Revisit when

Owners ask for automatic status. That would need an explicit per-agent opt-in, because prompts can hold
anything the user typed.
