# grok-gateway

An OpenAI-compatible gateway and backend for Grok CLI OAuth accounts. It automatically reads accounts from 9Router's SQLite database, rotates them using least-recently-used (LRU) scheduling, and locks an account for 24 hours whenever Grok reports `subscription:free-usage-exhausted`.

The gateway talks directly to Grok's Responses API (`https://cli-chat-proxy.grok.com/v1/responses`). It acts as a full standalone OpenAI backend for coding agents, web UIs, and client applications.

## Key Features

- **Zero Third-Party Runtime Dependencies**: Built with Python 3.10+ standard library only.
- **Thinking & Reasoning Support**: Streams `choices[0].delta.reasoning_content` and populates `choices[0].message.reasoning_content` for non-streaming requests.
- **Accurate Usage Tracking**: Maps upstream reasoning tokens directly into `usage.completion_tokens_details.reasoning_tokens`.
- **Robust Tool & Function Calling**: Two-way translation supporting modern OpenAI tool schemas, legacy `functions`, and `parallel_tool_calls`. Emits opening tool call chunks reliably even when preceded by chain-of-thought thinking.
- **Standard OpenAI Endpoints & Aliases**: Supports `/v1/chat/completions`, `/chat/completions`, `/v1/models`, `/models`, and `/health`.
- **Web UI & Browser Ready**: Built-in CORS preflight (`OPTIONS`) handling for Open WebUI, Chatbox, Cherry Studio, etc.
- **Smart Quota Protection**: Auto-rotates accounts, locks exhausted accounts for 24 hours (or upstream reset time), and guarantees no duplicate execution prefix once output starts.
- **Concurrent In-Flight Isolation**: Actively reserved in-flight accounts ensure parallel requests never clash on the same account.

## Quickstart

### 1. Clone

```sh
git clone https://github.com/Lyravein/grok-gateway.git
cd grok-gateway
```

### 2. Configuration

Create your configuration directory and environment file (`chmod 0600`):

```sh
mkdir -p ~/.config/grok-gateway
cat << 'EOF' > ~/.config/grok-gateway/env
GATEWAY_API_KEY=your-secure-random-api-key
GROK_DB_PATH=/path/to/your/.9router/db/data.sqlite
GATEWAY_STATE=/path/to/your/.local/state/grok-gateway/state.json
GATEWAY_HOST=127.0.0.1
GATEWAY_PORT=8787
GROK_MAX_ATTEMPTS=8
GROK_TIMEOUT=300
EOF
chmod 600 ~/.config/grok-gateway/env
```

> **Note**: `GATEWAY_API_KEY` is your own secret key used by clients to authenticate with this gateway. It is not an upstream Grok token.

### 3. Environment Variables Reference

| Variable | Default | Description |
|---|---|---|
| `GATEWAY_API_KEY` | *(Required)* | Secret bearer token required to authenticate requests to the gateway |
| `GROK_DB_PATH` | *(Required)* | Path to 9Router's SQLite database (`~/.9router/db/data.sqlite`) |
| `GATEWAY_STATE` | `~/.local/state/grok-gateway/state.json` | Path where LRU rotation and account lockout state is persisted |
| `GATEWAY_HOST` | `127.0.0.1` | Local bind address (keep local: do not expose directly to public internet) |
| `GATEWAY_PORT` | `8787` | Port for the HTTP server |
| `GROK_MAX_ATTEMPTS` | `8` | Maximum number of account rotation attempts before failing |
| `GROK_TIMEOUT` | `300` | Upstream HTTP timeout in seconds |
| `GATEWAY_DEFAULT_EFFORT` | *(Unset)* | Default reasoning effort applied when request carries no effort or suffix (`low`, `medium`, `high`, `xhigh`, `minimal`) |

### 4. Run the Gateway

Ensure the state directory exists with secure permissions (`0700`):

```sh
mkdir -p ~/.local/state/grok-gateway
chmod 700 ~/.local/state/grok-gateway

# Load environment and start
set -a; . ~/.config/grok-gateway/env; set +a
python3 gateway.py
```

The default listener is `127.0.0.1:8787`. Do not expose it directly to the public internet.

Verify that the gateway is responding:

```sh
# Health check (no auth required)
curl -s http://127.0.0.1:8787/health

# List supported models
curl -s http://127.0.0.1:8787/v1/models \
  -H "Authorization: Bearer your-secure-random-api-key"

# Chat completion with reasoning
curl -s http://127.0.0.1:8787/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-secure-random-api-key" \
  -d '{
    "model": "grok-4.7-high",
    "messages": [{"role": "user", "content": "Explain gravity in one sentence."}],
    "max_tokens": 128
  }'
```

## Client Integration

### Direct OpenAI Clients (OpenCode, Hermes, Cursor, Chatbox, etc.)

Configure your client to point to the local gateway:
- **Base URL**: `http://127.0.0.1:8787/v1` (or `http://127.0.0.1:8787`)
- **API Key**: The value of your `GATEWAY_API_KEY`
- **Supported Models**: `grok-4.7`, `grok-4.7-high`, `grok-4.7-xhigh`, `grok-4.7-medium`, `grok-4.7-low`, `grok-4.6`, `grok-4.6-high`, `grok-4.5`
- **Reasoning / Thinking**: Supported out of the box via `choices[0].delta.reasoning_content`.
- **Reasoning Effort Control**: Effort is controlled via the `reasoning` field the gateway forwards upstream. The client-side `reasoning_effort` parameter ("low", "medium", "high", "xhigh"), the `reasoning: {effort: "high"}` object, and model name suffixes (e.g. `grok-4.7-low`, `grok-4.7-high`) are all accepted client-side spellings that map to the same upstream field. Effort is a soft control and reasoning cannot be fully disabled on grok-4.7, 4.6, or 4.5. Note that the variant IDs listed by `/v1/models` are convenience client-side names that the gateway translates into base models with reasoning effort; they are not sent to the upstream Responses endpoint, which rejects them (HTTP 404). Default gateway-side effort for requests without any effort can be configured via `GATEWAY_DEFAULT_EFFORT`.

### Wire into 9Router

You can also use grok-gateway as a custom provider inside 9Router:
1. Create an OpenAI-compatible custom provider in 9Router:
   - **Base URL**: `http://127.0.0.1:8787/v1`
   - **API Key**: The value of `GATEWAY_API_KEY`
   - **Models**: `grok-4.7`, `grok-4.7-high`, `grok-4.7-xhigh`
2. Route desired combos to this custom provider.
3. Keep the original `grok-cli` connections active in 9Router so 9Router can refresh their OAuth tokens automatically in the background.

## Systemd User Service

To run grok-gateway in the background as a systemd user service:

1. Copy the service template:
   ```sh
   mkdir -p ~/.config/systemd/user
   cp deploy/grok-gateway.service ~/.config/systemd/user/
   ```

2. Edit `~/.config/systemd/user/grok-gateway.service` if your repository is placed in a custom directory. The template defaults to `%h/grok-gateway`.

3. Enable and start the service:
   ```sh
   systemctl --user daemon-reload
   systemctl --user enable --now grok-gateway.service
   systemctl --user status grok-gateway.service
   ```

## Development and Testing

Tests run against a temporary SQLite database and local fake upstream servers. No real accounts or internet connections are used:

```sh
python3 -m pytest -q
```

## Security & Architecture Notes

- **Credentials Safety**: The gateway reads `providerConnections` from 9Router in read-only mode. Access tokens are never logged, never stored in the state file, and never returned in API responses.
- **State Directory Permissions**: The gateway requires that `GATEWAY_STATE` and its directory have strict user-only permissions (`0700` / `0600`) and will refuse to start if accessible by others.
- **Fail-Safe Retries**: Requests are only retried across accounts before any output has been streamed to the client. Once tokens or tool calls start, retries are locked to prevent duplicate prefixes or corrupted outputs.
