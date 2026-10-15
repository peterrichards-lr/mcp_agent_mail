STATUS: Accepted 2026-10-15 (not built yet: #5)

# 0002. Capacity-pool scope is chosen per agent at registration, defaulting to the project

## Decision

An agent can offer spare capacity to other agents ("I have this requirement, does anyone have capacity?").
Its owner chooses how far that offer reaches when registering it: one project (repo), a product (a group of
repos), or the whole server. The default is the project.

Owner, 2026-10-15, asked which agents should share a pool:

> "I think it is a combinatoin of Product (group of repos), Single project (repo) and Whole server. Again, I
> think this is something the agent owner configured when they first register their agent with the server.
> The default should be Single project (repo) as this will expose the agent to fewer requests"

## Cost

- Three scopes to build, explain and test instead of one.
- An agent registered with the default never sees requests from neighbouring repos, even when it's idle and could help. Owners have to opt in to wider scopes.

## Enforced by

Not yet: the capacity pool is issue #5. Per [0001](0001-token-limits-are-enforced-by-agents.md), the
amount offered is the agent's own declaration.

Per the "Forget me" design (#4), scope is set at registration alongside the other registration-time
settings. The data model for forgetting has to be settled before the pool is built.

## Revisit when

The pool is built and the default turns out to leave most capacity unused, or wider scopes turn out to flood
agents with requests.
