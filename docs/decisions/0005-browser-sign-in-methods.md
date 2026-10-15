STATUS: Accepted 2026-10-15

# 0005. Browser sign-in: GitHub, password with mandatory MFA, company SSO; the token is a fallback

## Decision

People sign in to the web UI with GitHub, with a username and password plus a mandatory authenticator code,
or with company SSO (OpenID Connect). The server's bearer token also works, as an admin fallback that can be
turned off. With `HTTP_UI_SSO_ONLY`, company SSO becomes the only way in.

Owner, 2026-10-15. First, on browser access to the hosted server, the owner chose **"Token login + cookie (Recommended)"**. Then:

> "Is there a way for me to setup a login rather than needing the bearer token from the server?"

> "I would go with GitHub sign-in"

> "However, that does mean this tool is for development only. What about general requests? It might be worth
> having username / password too. What do you think? Perhaps MFA if we have username / password"

The session made MFA mandatory rather than optional. Company SSO followed from the lfr-tunnel review, and the
owner approved it with "Ok go with 9 and 12, followed by 15" (#9).

## Cost

- Four sign-in paths to keep secure and tested.
- Password accounts mean storing password hashes and TOTP secrets, plus lockout and recovery codes. Operators create accounts from the CLI, since there's no self-service sign-up.

## Enforced by

- `src/mcp_agent_mail/http.py`: `_ui_user_allowed` re-checks the user on every request, and the sign-in routes live under `/mail/auth/`.
- `src/mcp_agent_mail/ui_auth.py` and `src/mcp_agent_mail/oidc.py`.
- Tests: `test_mail_ui_login`, `test_ui_password_login`, `test_oidc_login` and `test_sessions_and_audit`.

## Revisit when

An organisation runs the server with SSO only. Then the other paths are attack surface with no users, and
`HTTP_UI_SSO_ONLY` should become that deployment's default.
