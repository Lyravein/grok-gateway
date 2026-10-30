# grok-gateway

[OI]-compatible gateway for Grok CLI OAuth accounts. It reads accounts from 9Router's SQLite database, rotates them by least recent use, and locks an account for 24 hours when Grok reports `subscription:free-usage-exhausted`.

The gateway calls `https://cli-chat-proxy.grok.com/v1/responses` directly. It is not a reverse proxy in front of 9Router: account selection happens here, where the credentials are available.

## Properties

- Python 3.10+ standard library only.
- Full OpenAI-compatible backend: `/v1/chat/completions`, `/chat/completions`, `/v1/models`, `/models`, and `/health`.
- Thinking / reasoning support: streams `choices[0].delta.reasoning_content` and populates `choices[0].message.reasoning_content` in non-streaming responses.
- Accurate usage metrics: maps reasoning tokens to `usage.completion_tokens_details.reasoning_tokens`.
- Two-way tool/function calling: translates OpenAI nested tool schemas and legacy `functions` into Grok Responses format, and emits opening tool call chunks reliably even when preceded by reasoning thoughts.
- CORS preflight (`OPTIONS`) support for browser-based web UIs.
- Flexible reasoning effort control: via `reasoning_effort` ("low", "medium", "high", "xhigh"), `reasoning.effort` object, or model suffix (`grok-4.7-high`).
- `max_tokens` and `max_completion_tokens` forwarded as `max_output_tokens`.
- Known-exhausted accounts are skipped before any upstream request.
- A request is retried only before any upstream output has been accepted. Once reasoning, text, or tool-call output starts, there is no replay and no duplicated prefix.
- In-flight accounts are reserved, so concurrent requests never share one account.
- Credentials are read-only and never written to logs or gateway state.
- One gateway process per state file.

## Clone

```sh
git clone https://github.com/Lyravein/grok-gateway.git /home/lyravein/workspace/grok-gateway
```

## Configure

Create `/home/lyravein/.config/grok-gateway/env` with mode `0600`:

```sh
GATEWAY_API_KEY=replace-with-a-long-random-value
GROK_DB_PATH=/home/lyravein/.9router/db/data.sqlite
GATEWAY_STATE=/home/lyravein/.local/state/grok-gateway/state.json
GATEWAY_HOST=127.0.0.1
GATEWAY_PORT=8787
GROK_MAX_ATTEMPTS=8
GROK_TIMEOUT=300
```

`GATEWAY_API_KEY` is the key clients use to call the gateway. It is not a Grok token.

9Router must remain responsible for refreshing the OAuth tokens. The gateway re-reads `providerConnections` every 30 seconds and accepts only rows where `provider="grok-cli"`, `isActive=1`, and `data.accessToken` is a non-empty string.

## Run

```sh
mkdir -p /home/lyravein/.local/state/grok-gateway
chmod 700 /home/lyravein/.local/state/grok-gateway
set -a; . /home/lyravein/.config/grok-gateway/env; set +a
/usr/bin/python3 /home/lyravein/workspace/grok-gateway/gateway.py
```

The default listener is `127.0.0.1:8787`. Do not expose it directly to the public internet.

Check it:

```sh
curl -s http://127.0.0.1:8787/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer replace-with-a-long-random-value' \
  -d '{"model":"grok-4.7-high","messages":[{"role":"user","content":"reply with ok"}],"max_tokens":32}'
```

## Wire into 9Router

Create an [OI]-compatible custom provider in 9Router with:

- Base URL: `http://127.0.0.1:8787/v1`
- API key: the value of `GATEWAY_API_KEY`
- Models: `grok-4.7`, `grok-4.7-high`, `grok-4.7-xhigh`, and other variants supported by the upstream account

Route the desired combo to that custom provider. Keep the original `grok-cli` connections enabled so 9Router can refresh their tokens. Do not route the same combo to both the original `grok-cli` provider and this gateway unless double quota use is intended.

Changing the live 9Router provider configuration is a separate operation and is not performed by this repository's tests.

## Direct OpenAI Client Setup

You can point any OpenAI-compatible client directly to the gateway:

- **Base URL**: `http://127.0.0.1:8787/v1` (or `http://127.0.0.1:8787`)
- **API Key**: the value of `GATEWAY_API_KEY`
- **Models**: `grok-4.7`, `grok-4.7-high`, `grok-4.7-xhigh`, `grok-4.6`
- **Thinking / Reasoning Display**: Supported automatically via standard `delta.reasoning_content` (streaming) and `message.reasoning_content` (non-streaming).
- **Tool Calling**: Fully supported with function execution and multi-turn loops.

## systemd user service

The template is `deploy/grok-gateway.service`. Install it after reviewing the paths:

```sh
mkdir -p /home/lyravein/.config/systemd/user
cp deploy/grok-gateway.service /home/lyravein/.config/systemd/user/grok-gateway.service
systemctl --user daemon-reload
systemctl --user enable --now grok-gateway.service
systemctl --user status grok-gateway.service
```

The unit expects a clone at `/home/lyravein/workspace/grok-gateway` and the environment file shown above. Edit the unit first if the clone lives elsewhere.

## Tests

```sh
/usr/bin/python3 -m pytest -q
```

The suite uses temporary SQLite databases and local fake upstream servers. It never reads real credentials or contacts Grok.

## Limitations

- OAuth login and token refresh are intentionally not implemented.
- Only one gateway process may own a state file.
- A rolling 24-hour upstream window may reset earlier than the conservative 24-hour lock. If upstream supplies a future numeric reset timestamp, the gateway uses it.
- The gateway translates text and function calls only. Images, audio, and provider-specific extensions are out of scope.
- Corrupt gateway state is preserved as `state.json.corrupt-<timestamp>` and replaced with empty state. 9Router's database is never modified.
