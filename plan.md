# grok-gateway implementation plan

## Goal

Provide an [OI]-compatible HTTP gateway for Grok CLI OAuth accounts. The gateway owns account rotation, skips accounts already known to be exhausted, and locks an account for 24 hours when Grok returns `subscription:free-usage-exhausted`, retrying the next account without repeating a request against the exhausted account.

## Current facts and assumptions

- Grok upstream: `https://cli-chat-proxy.grok.com/v1/responses`.
- Account credentials currently live in 9Router's SQLite DB, table `providerConnections`, rows with `provider="grok-cli"` and `isActive=1`.
- The gateway reads credentials from that DB with SQLite URI `mode=ro`; it never writes to 9Router's DB and never copies tokens into the repository, state file, or logs.
- Credential extraction is exactly `SELECT id, data FROM providerConnections WHERE provider='grok-cli' AND isActive=1`, then `json.loads(data)["accessToken"]`. Only a non-empty string is accepted. Rows with malformed JSON or a missing/empty/non-string token are skipped and counted as unusable without logging the row id, email, or raw data.
- 9Router remains responsible for refreshing OAuth tokens. The gateway re-reads account data at most once every 30 seconds so refreshed access tokens are picked up.
- Quota lock is account-wide, not per model, because the same upstream account counter was observed across Grok models.
- Default lock after `subscription:free-usage-exhausted` is 24 hours. If an upstream response supplies a valid future reset timestamp, use that instead.
- Default bind address is `127.0.0.1`; a bearer `GATEWAY_API_KEY` is mandatory.
- Python 3.10+ standard library only at runtime. Tests use the already installed pytest.

## Architecture

One stdlib `ThreadingHTTPServer` accepts [OI] chat completion requests. It converts chat messages to Responses API input, selects the least-recently-used eligible account, and calls Grok with `stream=true` and `max_output_tokens` mapped from the client's `max_tokens`.

For non-streaming clients the gateway buffers one upstream response and returns one [OI] JSON response. For streaming clients it withholds the client response until the first convertible upstream output arrives. A retry is allowed only before any upstream text or tool-call output has been accepted. Once output starts, the gateway never retries, preventing duplicate answers or tool calls.

Persistent state contains only account id, last-used timestamp, exhausted-until timestamp, and counters. Tokens and request bodies are never written to state or logs.

## Concurrency and state model

- Exactly one gateway process may own a state file. Multiple processes sharing one state file are unsupported and rejected by the deployment documentation.
- A single `threading.RLock` protects eligibility filtering, in-flight reservations, least-recently-used selection, last-used updates, exhaustion locks, cooldowns, and every state-file write.
- Selection, insertion into an in-flight account-id set, and its last-used update happen in the same critical section, before the upstream request. An in-flight account is ineligible for every other request and is removed from that set in a `finally` block. Concurrent requests therefore never share one account.
- If no account is eligible because all otherwise eligible accounts are in flight, return HTTP 503 with `Retry-After: 1`; this is distinct from the all-exhausted HTTP 429 case.
- State mutations are serialized under the same lock. A lost update must not clear another request's exhaustion lock.
- State is written to a same-directory temporary file opened with mode `0600`, flushed and `fsync`ed, then installed with `os.replace`. The state directory is created with mode `0700`.
- Missing state means empty state. Corrupt state is preserved by renaming it to `state.json.corrupt-<unix timestamp>`, a generic error is logged without file contents, and the gateway starts with empty state. State must never contain access tokens, refresh tokens, email addresses, request text, or response text.
- The gateway refuses to start if the state file exists and is group/world-readable or writable. Deployment requires the environment file to be mode `0600`.

## Credential and transport security

- Client authentication uses `Authorization: Bearer <GATEWAY_API_KEY>` and `hmac.compare_digest`. Missing or wrong credentials receive the same generic HTTP 401 JSON error.
- Upstream authentication is exactly `Authorization: Bearer <accessToken>`; tokens never appear in URLs.
- Only the configured HTTPS upstream origin is allowed. The implementation uses `ssl.create_default_context()` with hostname checking and `CERT_REQUIRED`; there is no option to disable verification.
- Logs may contain request id, attempt number, model, upstream status, lock expiry, and elapsed time. Logs must not contain account ids, emails, access tokens, refresh tokens, API keys, request bodies, response bodies, or authorization headers.
- Tests explicitly assert that sentinel credentials are absent from logs and state.

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
- Request bodies larger than 16 MiB are rejected with HTTP 413 before reading the upstream DB.

## Exact Responses SSE to [OI] mapping

- `response.output_text.delta` appends its `delta` string to chat `choices[0].delta.content`.
- `response.output_text.done` supplies text only when no text delta for that output item was accepted, preventing duplicated text.
- `response.output_item.added` with `item.type="function_call"` allocates the next chat tool index and records it by upstream `output_index` and `item.id`. It emits one initial chat tool call containing `index`, `id=item.call_id or item.id`, `type="function"`, `function.name`, and empty arguments. If neither id nor name exists, this is a protocol error; no placeholder identity is invented.
- `response.function_call_arguments.delta` resolves its call by `output_index`, falling back to `item_id`. It appends the argument delta to that exact call. An unknown reference is a protocol error, not a silently dropped argument.
- `response.output_item.done` for a function call resolves the existing call by event `output_index` first and `item.id` second. If both are present they must reference the same call; disagreement is a protocol error. It uses full `item.arguments` only when no argument deltas were accepted, validates that the complete arguments are JSON, and marks that call complete. Interleaved calls retain independent indexes and argument buffers.
- `response.completed` yields finish reason `tool_calls` if at least one complete function call exists, otherwise `stop`. If any started call remains incomplete, or if any referenced event is malformed, the response is a protocol failure rather than a fabricated successful finish.
- Usage from `response.completed` is copied when available. Successful streams end with `data: [DONE]`.
- On malformed SSE, invalid JSON event data, unknown tool-call references, or incomplete tool calls after output has started, the gateway logs a generic protocol error and aborts the stream without a successful finish or `[DONE]`. It never replays the request.
- Before any output, `response.failed`, `error`, malformed terminal events, and non-2xx responses are inspected for the exhaustion code and may trigger account rotation according to the failure policy.

## Failure policy

- Before selecting an account, exclude ids whose `exhausted_until` is in the future.
- Exhaustion detection checks non-2xx JSON/text bodies and pre-output SSE error events for case-insensitive `subscription:free-usage-exhausted` or `free-usage-exhausted`.
- On exhaustion, persist the lock immediately and retry the next account if no upstream output was accepted.
- Other retryable 429/5xx failures use a short in-memory cooldown and may try the next account, up to `GROK_MAX_ATTEMPTS` total attempts.
- Never retry a 400/401/403/404 request as though it were quota exhaustion.
- If all accounts are locked, return HTTP 429 with the earliest known reset and no credential details.
- If output has started, upstream failures are surfaced once; no replay and no duplicated prefix.
- Maximum attempts default to 8. Upstream timeout is configurable, default 300 seconds.

## Files

- `gateway.py`: configuration, account store, lock/retry policy, protocol conversion, HTTP handler, CLI entry point.
- `test_gateway.py`: unit and local fake-upstream integration tests, written before implementation.
- `README.md`: configuration, run command, 9Router custom-provider wiring, single-process limitation.
- `deploy/grok-gateway.service`: systemd unit template using an environment file outside the repo.
- `.gitignore`: Python caches, local state, env files.

## Tasks and verification

1. Write failing tests for account filtering, persistent 24-hour lock, least-recently-used rotation, retry-on-exhausted, and no-retry-after-output. Run `/usr/bin/python3 -m pytest -q` and confirm the tests fail because the implementation is absent.
2. Add tests for 20 concurrent selections, proving last-used selection/update atomicity and preservation of concurrent lock updates. Add corrupt-state recovery and state-permission tests.
3. Add protocol conversion tests for system/user/assistant/tool history, streamed text, two interleaved function calls, unknown argument references, malformed JSON, incomplete calls, and missing ids. Require no fabricated call and no successful `[DONE]` on protocol failure.
4. Add HTTP tests for missing/wrong bearer key, oversized bodies, exhaustion in non-SSE and pre-output SSE errors, retry before output, and no retry after output.
5. Add security tests with sentinel tokens proving absence from state and logs, and assert TLS context hostname checking plus `CERT_REQUIRED`.
6. Implement `gateway.py` minimally until all tests pass. Run `/usr/bin/python3 -m pytest -q` and require all green.
7. Add README and systemd template. Run `python3 -m py_compile gateway.py` and the full suite.
8. On the VPS, run a local smoke test against the real DB and one inexpensive real request. Verify from redacted gateway logs that an account known as exhausted is skipped without an upstream call.
9. Architect reviews this revised plan and the final diff after tests. Any serious finding goes back for correction before completion.

## Out of scope

- Replacing 9Router for other providers.
- Implementing OAuth login or token refresh.
- Writing to 9Router's database.
- A web dashboard.
- Multiple gateway processes sharing one state file.
- Automatic deployment or changing the live 9Router provider configuration without explicit user approval.
- Retry after any upstream text or tool-call output has been accepted.
