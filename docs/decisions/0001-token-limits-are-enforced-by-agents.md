STATUS: Accepted 2026-10-15

# 0001. Token limits are enforced on the agent's machine, never by the server

## Decision

Each agent's owner decides how much of their plan other agents' mail may spend. The limits are
checked on the owner's machine, by the hooks that wake the agent.

Owner, 2026-10-15:

> "perhaps we should allow the people who own each agent to specify the number of tokens that external
> requests can consume, this could be a fixed limit or a percentage of the tokens available."

> "Yes the token restructions will be on an agent basis not at the server level. However, the agent can
> tell the server, this is what I can offer the pool"

Asked how a limit should decide whether mail may wake an agent (plan-usage percentage, wakes per hour, or both), the owner chose **"Both"**.

## Cost

- The server can't stop a misconfigured or modified client from spending more. The limits protect the owner from other agents, not from themselves.
- Usage is only as fresh as the client's status line reports it. When no status line runs, only the wake cap applies.

## Enforced by

- `scripts/hooks/mail_wake.py`: `_limit_reason` checks `AGENT_MAIL_WAKE_MAX_USAGE_PCT` and `AGENT_MAIL_WAKE_MAX_PER_HOUR`, with plan usage tracked per account.
- `tests/test_mail_wake.py`: the usage-limit, per-account and wake-cap tests.
- The second half of the owner's words, where an agent tells the server what it can offer the pool, is the capacity pool (#5). It isn't built yet.

## Revisit when

The server has to arbitrate between agents, for example to share one capacity pool fairly. It would then need
each agent's declared offer, but still not the authority to spend more than that.
