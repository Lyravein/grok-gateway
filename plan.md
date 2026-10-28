# grok-gateway implementation plan

## Goal

Provide an [OI]-compatible HTTP gateway for Grok CLI OAuth accounts. The gateway owns account rotation, skips accounts already known to be exhausted, and locks an account for 24 hours when Grok returns `subscription:free-usage-exhausted`, retrying the next account without repeating a request against the exhausted account.

## Current facts and assumptions

- Grok upstream: `https://cli-chat-proxy.grok.com/v1/responses`.
- Account credentials currently live in 9Router's SQLite DB, table `providerConnections`, rows with `provider="grok-cli"` and `isActive=1`.
- The gateway reads credentials from that DB with SQLite URI `mode=ro`; it never writes to 9Router's DB and never copies tokens into the repository or logs.
- 9Router remains responsible for refreshing OAuth tokens. The gateway re-reads account data at most once every 30 seconds so refreshed access tokens are picked up.
- Quota lock is account-wide, not per model, because the same upstream account counter was observed across Grok models.
- Default lock after `subscription:free-usage-exhausted` is 24 hours. If an upstream response supplies a usable reset timestamp, use that instead.
- Default bind address is `127.0.0.1`; a bearer `GATEWAY_API_KEY` is mandatory.
- Python 3.10+ standard library only at runtime. Tests use the already installed pytest.

## Architecture

One stdlib `ThreadingHTTPServer` accepts [OI] chat completion requests. It converts chat messages to Responses API input, selects the least-recently-used eligible account, and calls Grok with `stream=true` and `max_output_tokens` mapped from the client's `max_tokens`.

For non-streaming clients the gateway buffers one upstream response and returns one [OI] JSON response. For streaming clients it withholds the client response until the first convertible upstream output arrives. This permits retrying another account on an upstream failure only when no content has been emitted. Once content starts, the gateway never retries, preventing duplicate answers or tool calls.

Persistent state contains only account id, last-used timestamp, exhausted-until timestamp, and counters. It is stored as atomically replaced JSON outside git. Tokens and request bodies are never written to state or logs.

## Files

- `gateway.py`: configuration, account store, lock/retry policy, protocol conversion, HTTP handler, CLI entry point.
- `test_gateway.py`: unit and local fake-upstream integration tests, written before implementation.
- `README.md`: configuration, run command, 9Router custom-provider wiring, limitations.
- `deploy/grok-gateway.service`: systemd unit template using an environment file outside the repo.
- `.gitignore`: Python caches, local state, env files.

## Request conversion rules

- Join system and developer text into Responses `instructions`.
- User text becomes `input_text` content.
- Assistant text becomes `output_text` content.
- Assistant `tool_calls` become separate `function_call` items using the original `call_id`, name, and arguments.
- Tool results become `function_call_output` items with matching `call_id`.
- [OI] function tools are flattened to Responses function tools.
- `max_tokens` becomes `max_output_tokens`; this must not be dropped.
- Model suffixes `-high` and `-xhigh` select reasoning effort and are removed from the upstream model name.
- Unsupported [OI] sampling fields are not invented or silently translated; only known safe fields are forwarded.

## Response conversion rules

- `response.output_text.delta` becomes chat `delta.content`.
- Function-call items become indexed chat `tool_calls`; argument deltas append to the same tool call.
- Finish reason is `tool_calls` when at least one complete function call exists, otherwise `stop`.
- Usage from `response.completed` is copied when available.
- SSE frames always end with `data: [DONE]` after a successful stream.
- Non-stream responses contain complete `message.content`, `message.tool_calls`, `finish_reason`, and `usage`.

## Failure policy

- Before selecting an account, exclude ids whose `exhausted_until` is in the future.
- On `subscription:free-usage-exhausted`, persist the lock immediately and retry the next account if no client output was emitted.
- Other retryable 429/5xx failures use a short in-memory cooldown and may try the next account, up to `GROK_MAX_ATTEMPTS` total attempts.
- Never retry a 400/401/403/404 request as though it were quota exhaustion.
- If all accounts are locked, return HTTP 429 with the earliest known reset and no credential details.
- If output has started, upstream failures are surfaced once; no replay and no duplicated prefix.
- Maximum request body: 16 MiB. Upstream timeout: configurable, default 300 seconds.

## Tasks and verification

1. Write failing tests for account filtering, persistent 24-hour lock, least-recently-used rotation, retry-on-exhausted, and no-retry-after-output. Run `/usr/bin/python3 -m pytest -q` and confirm the tests fail because the implementation is absent.
2. Implement account store, configuration, and HTTP server in `gateway.py`. Re-run the same tests and require all green.
3. Add protocol conversion tests for system/user/assistant/tool history, streamed text, and streamed function calls. Require all green.
4. Add README and systemd template. Run a syntax check and the full suite.
5. On the VPS, run a local smoke test against the real DB and one inexpensive real request. Verify from gateway logs that an account known as exhausted is skipped without an upstream call.
6. Architect reviews the plan before implementation and the final diff after tests. Any serious finding goes back for correction before completion.

## Out of scope

- Replacing 9Router for other providers.
- Implementing OAuth login or token refresh.
- Writing to 9Router's database.
- A web dashboard.
- Automatic deployment or changing the live 9Router provider configuration without explicit user approval.
- Retry after any content has already been sent to the client.
