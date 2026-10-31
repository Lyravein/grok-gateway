import http.client
import json
import os
import sqlite3
import ssl
import stat
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from gateway import (
    LOCK_SECONDS,
    Account,
    AccountStore,
    NoAccountAvailable,
    ProtocolError,
    ReasoningCache,
    ResponsesTranslator,
    load_accounts,
    make_server,
    make_tls_context,
    openai_usage,
    translate_messages,
    translate_request,
)


def account(name):
    return Account(id=name, access_token="token-" + name)


def sse(event):
    return "data: " + json.dumps(event) + "\n\n"


def text_events(text="ok"):
    return [
        sse({"type": "response.output_text.delta", "delta": text}),
        sse({"type": "response.completed", "response": {"usage": {
            "input_tokens": 10, "output_tokens": 2, "total_tokens": 12,
        }}}),
        "data: [DONE]\n\n",
    ]


class FakeUpstream:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                payload = json.loads(self.rfile.read(length))
                outer.calls.append({
                    "authorization": self.headers.get("Authorization"),
                    "user_agent": self.headers.get("User-Agent"),
                    "client_version": self.headers.get("x-grok-client-version"),
                    "client_identifier": self.headers.get("x-grok-client-identifier"),
                    "token_auth": self.headers.get("x-xai-token-auth"),
                    "client_mode": self.headers.get("x-grok-client-mode"),
                    "payload": payload,
                })
                status, body = outer.handler(len(outer.calls), payload)
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                self.wfile.write(body.encode())
                self.wfile.flush()

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return "http://127.0.0.1:%d/v1/responses" % self.server.server_address[1]

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


class RunningGateway:
    def __init__(self, store, upstream_url, **kwargs):
        self.server = make_server(
            "127.0.0.1", 0, store, upstream_url,
            api_key="gateway-secret", timeout=5, **kwargs,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def post(self, body, key="gateway-secret"):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        headers = {"Content-Type": "application/json"}
        if key is not None:
            headers["Authorization"] = "Bearer " + key
        conn.request("POST", "/v1/chat/completions", json.dumps(body), headers)
        response = conn.getresponse()
        raw = response.read().decode()
        conn.close()
        return response.status, raw

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.fixture
def store(tmp_path):
    return AccountStore(
        [account("a"), account("b"), account("c")],
        tmp_path / "state.json",
        clock=lambda: 1000.0,
    )


def chat(stream=False):
    return {
        "model": "grok-4.7-high",
        "messages": [{"role": "user", "content": "say ok"}],
        "stream": stream,
        "max_tokens": 40,
    }


def test_load_accounts_reads_only_valid_active_access_tokens(tmp_path):
    path = tmp_path / "db.sqlite"
    db = sqlite3.connect(path)
    db.execute("CREATE TABLE providerConnections (id TEXT, provider TEXT, isActive INTEGER, data TEXT)")
    rows = [
        ("a", "grok-cli", 1, json.dumps({"accessToken": "good"})),
        ("bad-json", "grok-cli", 1, "{"),
        ("missing", "grok-cli", 1, json.dumps({"refreshToken": "no"})),
        ("empty", "grok-cli", 1, json.dumps({"accessToken": ""})),
        ("inactive", "grok-cli", 0, json.dumps({"accessToken": "no"})),
        ("other", "other", 1, json.dumps({"accessToken": "no"})),
    ]
    db.executemany("INSERT INTO providerConnections VALUES (?,?,?,?)", rows)
    db.commit()
    db.close()

    assert load_accounts(path) == [Account(id="a", access_token="good")]


def test_exhausted_account_is_locked_persistently_and_skipped(store, tmp_path):
    store.mark_exhausted("a")
    assert store.select().id == "b"
    raw = (tmp_path / "state.json").read_text()
    assert "token-a" not in raw
    assert stat.S_IMODE(os.stat(tmp_path / "state.json").st_mode) == 0o600

    reloaded = AccountStore(
        [account("a"), account("b")], tmp_path / "state.json", clock=lambda: 1001.0,
    )
    assert reloaded.select().id == "b"


def test_lock_expires(store):
    store.mark_exhausted("a", reset_at=1001.0)
    assert store.select().id == "b"
    store.clock = lambda: 1002.0
    assert store.select().id == "a"


def test_rotation_uses_least_recently_used(store):
    assert store.select().id == "a"
    store.release("a")
    assert store.select().id == "b"
    store.release("b")
    assert store.select().id == "c"


def test_state_directory_must_be_private(store, tmp_path):
    os.chmod(tmp_path, 0o777)
    with pytest.raises(RuntimeError):
        store.check_permissions()
    os.chmod(tmp_path, 0o700)


def test_millisecond_reset_timestamp_falls_back_to_24_hours(store):
    lock_until = store.mark_exhausted("a", body=json.dumps({"reset_at": 1793159000000}))
    assert lock_until == 1000.0 + LOCK_SECONDS


def test_corrupt_state_is_preserved_without_reading_contents(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{ definitely-not-json")
    recovered = AccountStore([account("a")], path, clock=lambda: 1.0)
    assert recovered.select().id == "a"
    backups = list(tmp_path.glob("state.json.corrupt-*"))
    assert len(backups) == 1
    assert backups[0].read_text() == "{ definitely-not-json"


def test_inflight_accounts_are_never_shared(store):
    all_selected = threading.Event()
    release_all = threading.Event()
    append_lock = threading.Lock()
    selected = []

    def choose():
        chosen = store.select()
        with append_lock:
            selected.append(chosen.id)
            if len(selected) == 3:
                all_selected.set()
        assert release_all.wait(timeout=2)
        store.release(chosen.id)

    threads = [threading.Thread(target=choose) for _ in range(3)]
    for thread in threads:
        thread.start()
    assert all_selected.wait(timeout=2)
    assert sorted(selected) == ["a", "b", "c"]
    with pytest.raises(NoAccountAvailable) as error:
        store.select()
    assert error.value.kind == "busy"
    release_all.set()
    for thread in threads:
        thread.join(timeout=2)
    assert all(not thread.is_alive() for thread in threads)


def test_request_translation_preserves_tools_history_and_token_cap():
    body = {
        "model": "grok-4.7-xhigh",
        "max_tokens": 123,
        "messages": [
            {"role": "system", "content": "be exact"},
            {"role": "user", "content": "read it"},
            {"role": "assistant", "content": None, "tool_calls": [{
                "id": "call-1", "type": "function",
                "function": {"name": "read", "arguments": "{\"path\":\"a\"}"},
            }]},
            {"role": "tool", "tool_call_id": "call-1", "content": "contents"},
        ],
        "tools": [{"type": "function", "function": {
            "name": "read", "description": "read file",
            "parameters": {"type": "object"},
        }}],
    }
    result = translate_request(body)
    assert result["model"] == "grok-4.7"
    assert result["reasoning"] == {"effort": "xhigh"}
    assert result["max_output_tokens"] == 123
    assert result["instructions"] == "be exact"
    assert result["input"][1] == {
        "type": "function_call", "call_id": "call-1",
        "name": "read", "arguments": "{\"path\":\"a\"}",
    }
    assert result["input"][2] == {
        "type": "function_call_output", "call_id": "call-1", "output": "contents",
    }
    assert result["tools"][0]["name"] == "read"
    assert result["tools"][0]["type"] == "function"
    assert "function" not in result["tools"][0]


def test_interleaved_tool_calls_keep_identity_and_arguments():
    translator = ResponsesTranslator()
    events = [
        {"type": "response.output_item.added", "output_index": 2, "item": {
            "type": "function_call", "id": "fc1", "call_id": "call1", "name": "alpha",
        }},
        {"type": "response.output_item.added", "output_index": 5, "item": {
            "type": "function_call", "id": "fc2", "call_id": "call2", "name": "beta",
        }},
        {"type": "response.function_call_arguments.delta", "output_index": 5, "delta": "{\"x\":"},
        {"type": "response.function_call_arguments.delta", "output_index": 2, "delta": "{\"a\":1}"},
        {"type": "response.function_call_arguments.delta", "output_index": 5, "delta": "2}"},
        {"type": "response.output_item.done", "output_index": 5, "item": {"id": "fc2"}},
        {"type": "response.output_item.done", "output_index": 2, "item": {"id": "fc1"}},
        {"type": "response.completed", "response": {"usage": {"total_tokens": 9}}},
    ]
    for event in events:
        translator.feed(event)
    assert translator.calls[0]["id"] == "call1"
    assert translator.calls[0]["arguments"] == "{\"a\":1}"
    assert translator.calls[1]["id"] == "call2"
    assert translator.calls[1]["arguments"] == "{\"x\":2}"
    assert translator.finish() == ("tool_calls", {"total_tokens": 9})


@pytest.mark.parametrize("events", [
    [{"type": "response.function_call_arguments.delta", "output_index": 99, "delta": "{}"}],
    [{"type": "response.output_item.added", "output_index": 1, "item": {
        "type": "function_call", "id": "fc1", "call_id": "c1", "name": "x",
    }}],
])
def test_malformed_or_incomplete_tool_call_is_protocol_error(events):
    translator = ResponsesTranslator()
    with pytest.raises(ProtocolError):
        if events[0]["type"] == "response.output_item.added":
            translator.feed(events[0])
            translator.finish()
        else:
            translator.feed(events[0])


def test_done_event_with_conflicting_identity_is_rejected():
    translator = ResponsesTranslator()
    translator.feed({"type": "response.output_item.added", "output_index": 1, "item": {
        "type": "function_call", "id": "fc1", "call_id": "c1", "name": "x",
    }})
    with pytest.raises(ProtocolError):
        translator.feed({"type": "response.output_item.done", "output_index": 1, "item": {"id": "other"}})


def test_tls_verification_cannot_be_disabled():
    context = make_tls_context()
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_stream_chunks_have_openai_shape_and_usage(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events("ok"))))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        lines = [line[6:] for line in raw.splitlines() if line.startswith("data: ")]
        assert lines[-1] == "[DONE]"
        chunks = [json.loads(line) for line in lines[:-1]]
        first = chunks[0]
        assert first["choices"][0]["index"] == 0
        assert first["choices"][0]["delta"] == {"content": "ok"}
        final = chunks[-1]
        assert final["choices"][0]["index"] == 0
        assert final["choices"][0]["finish_reason"] == "stop"
        assert final["usage"] == {
            "prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12,
        }
    finally:
        gateway.close()
        upstream.close()


def test_stream_empty_response_still_returns_valid_headers(store):
    events = [sse({"type": "response.completed", "response": {}}), "data: [DONE]\n\n"]
    upstream = FakeUpstream(lambda count, payload: (200, "".join(events)))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        assert "chat.completion.chunk" in raw
        assert "[DONE]" in raw
    finally:
        gateway.close()
        upstream.close()


def test_nonstream_usage_uses_openai_token_names(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events("ok"))))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat())
        assert status == 200
        assert json.loads(raw)["usage"] == {
            "prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12,
        }
    finally:
        gateway.close()
        upstream.close()


def test_unauthorized_request_never_reaches_upstream(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(), key=None)
        assert status == 401
        status2, _ = gateway.post(chat(), key="wrong")
        assert status2 == 401
        assert upstream.calls == []
        assert "gateway-secret" not in raw
    finally:
        gateway.close()
        upstream.close()


def test_exhaustion_locks_first_account_and_retries_next(store):
    def respond(count, payload):
        if count == 1:
            return 429, json.dumps({"code": "subscription:free-usage-exhausted"})
        return 200, "".join(text_events("fresh"))

    upstream = FakeUpstream(respond)
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat())
        assert status == 200
        assert json.loads(raw)["choices"][0]["message"]["content"] == "fresh"
        assert len(upstream.calls) == 2
        assert upstream.calls[0]["authorization"] == "Bearer token-a"
        assert upstream.calls[1]["authorization"] == "Bearer token-b"
        assert upstream.calls[1]["payload"]["max_output_tokens"] == 40
        assert upstream.calls[1]["user_agent"] == "grok-shell/1.0.44 (linux; x86_64)"
        assert upstream.calls[1]["client_version"] == "1.0.44"
        assert upstream.calls[1]["client_identifier"] == "grok-shell"
        assert upstream.calls[1]["token_auth"] == "xai-grok-cli"
        assert upstream.calls[1]["client_mode"] == "headless"

        status2, _ = gateway.post(chat())
        assert status2 == 200
        assert len(upstream.calls) == 3
        assert upstream.calls[2]["authorization"] == "Bearer token-b"
    finally:
        gateway.close()
        upstream.close()


def test_no_retry_after_stream_output_starts(store):
    body = (
        sse({"type": "response.output_text.delta", "delta": "partial"})
        + sse({"type": "response.failed", "response": {"error": {
            "code": "subscription:free-usage-exhausted",
        }}})
    )
    upstream = FakeUpstream(lambda count, payload: (200, body))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        assert "partial" in raw
        assert "[DONE]" not in raw
        assert len(upstream.calls) == 1
    finally:
        gateway.close()
        upstream.close()


def test_terminal_protocol_failure_returns_http_error(store):
    upstream = FakeUpstream(lambda count, payload: (200, "data: {broken}\n\n"))
    gateway = RunningGateway(store, upstream.url, max_attempts=1)
    try:
        status, raw = gateway.post(chat())
        assert status == 502
        assert json.loads(raw)["error"]["type"] == "upstream_error"
        assert len(upstream.calls) == 1
    finally:
        gateway.close()
        upstream.close()


def test_negative_content_length_is_rejected(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        conn.putrequest("POST", "/v1/chat/completions")
        conn.putheader("Authorization", "Bearer gateway-secret")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", "-1")
        conn.endheaders()
        response = conn.getresponse()
        assert response.status == 400
        response.read()
        conn.close()
        assert upstream.calls == []
    finally:
        gateway.close()
        upstream.close()


def test_tool_item_start_disables_retry(store):
    body = (
        sse({"type": "response.output_item.added", "output_index": 0, "item": {
            "type": "function_call", "id": "fc1", "call_id": "call1", "name": "read_file",
        }})
        + sse({"type": "response.failed", "response": {"error": {
            "code": "subscription:free-usage-exhausted",
        }}})
    )
    upstream = FakeUpstream(lambda count, payload: (200, body))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 502
        assert json.loads(raw)["error"]["type"] == "upstream_error"
        assert len(upstream.calls) == 1
    finally:
        gateway.close()
        upstream.close()


def test_oversized_body_is_rejected(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url, max_body_bytes=128)
    try:
        status, _ = gateway.post({"messages": [{"role": "user", "content": "x" * 500}]})
        assert status == 413
        assert upstream.calls == []
    finally:
        gateway.close()
        upstream.close()


def test_reasoning_deltas_are_translated_to_reasoning_content():
    translator = ResponsesTranslator()
    events = [
        {"type": "response.reasoning_text.delta", "output_index": 0, "delta": "Thinking step 1. "},
        {"type": "response.reasoning_summary_text.delta", "output_index": 0, "delta": "Step 2."},
        {"type": "response.output_text.delta", "output_index": 1, "delta": "Final answer."},
        {"type": "response.completed", "response": {"usage": {
            "input_tokens": 15, "output_tokens": 10, "total_tokens": 25,
            "output_tokens_details": {"reasoning_tokens": 8},
        }}},
    ]
    packets = []
    for event in events:
        packets.extend(translator.feed(event))

    assert translator.reasoning == "Thinking step 1. Step 2."
    assert translator.content == "Final answer."
    assert packets[0] == {"delta": {"reasoning_content": "Thinking step 1. "}}
    assert packets[1] == {"delta": {"reasoning_content": "Step 2."}}
    assert packets[2] == {"delta": {"content": "Final answer."}}

    finish_reason, usage = translator.finish()
    assert finish_reason == "stop"
    mapped_usage = openai_usage(usage)
    assert mapped_usage["completion_tokens_details"]["reasoning_tokens"] == 8


def test_reasoning_done_without_prior_deltas_emits_text():
    translator = ResponsesTranslator()
    packets = translator.feed({
        "type": "response.reasoning_text.done", "output_index": 0, "text": "Whole thought."
    })
    assert packets == [{"delta": {"reasoning_content": "Whole thought."}}]
    assert translator.reasoning == "Whole thought."


def test_stream_reasoning_starts_stream_immediately(store):
    events = [
        sse({"type": "response.reasoning_text.delta", "delta": "pondering"}),
        sse({"type": "response.output_text.delta", "delta": "result"}),
        sse({"type": "response.completed", "response": {}}),
        "data: [DONE]\n\n",
    ]
    upstream = FakeUpstream(lambda count, payload: (200, "".join(events)))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        lines = [line[6:] for line in raw.splitlines() if line.startswith("data: ")]
        assert lines[-1] == "[DONE]"
        chunks = [json.loads(line) for line in lines[:-1]]
        assert chunks[0]["choices"][0]["delta"] == {"reasoning_content": "pondering"}
        assert chunks[1]["choices"][0]["delta"] == {"content": "result"}
    finally:
        gateway.close()
        upstream.close()


def test_no_retry_after_reasoning_starts(store):
    events = (
        sse({"type": "response.reasoning_text.delta", "delta": "pondering"})
        + sse({"type": "response.failed", "response": {"error": {
            "code": "subscription:free-usage-exhausted",
        }}})
    )
    upstream = FakeUpstream(lambda count, payload: (200, events))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        assert "pondering" in raw
        assert "[DONE]" not in raw
        assert len(upstream.calls) == 1
    finally:
        gateway.close()
        upstream.close()


def test_streaming_reasoning_followed_by_tool_call_emits_opening_packet(store):
    events = [
        sse({"type": "response.reasoning_text.delta", "delta": "I need to call get_weather."}),
        sse({"type": "response.output_item.added", "output_index": 1, "item": {
            "type": "function_call", "id": "fc1", "call_id": "call1", "name": "get_weather",
        }}),
        sse({"type": "response.function_call_arguments.delta", "output_index": 1, "delta": "{\"loc\": \"PA\"}"}),
        sse({"type": "response.output_item.done", "output_index": 1, "item": {"id": "fc1"}}),
        sse({"type": "response.completed", "response": {}}),
        "data: [DONE]\n\n",
    ]
    upstream = FakeUpstream(lambda count, payload: (200, "".join(events)))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        lines = [line[6:] for line in raw.splitlines() if line.startswith("data: ")]
        assert lines[-1] == "[DONE]"
        chunks = [json.loads(line) for line in lines[:-1]]

        # Chunk 0 has reasoning
        assert chunks[0]["choices"][0]["delta"] == {"reasoning_content": "I need to call get_weather."}

        # Chunk 1 MUST have tool call definition (index, id, type, name)
        tool_call_chunk = chunks[1]["choices"][0]["delta"]["tool_calls"][0]
        assert tool_call_chunk["index"] == 0
        assert tool_call_chunk["id"] == "call1"
        assert tool_call_chunk["function"]["name"] == "get_weather"

        # Chunk 2 has arguments
        args_chunk = chunks[2]["choices"][0]["delta"]["tool_calls"][0]
        assert args_chunk["index"] == 0
        assert args_chunk["function"]["arguments"] == "{\"loc\": \"PA\"}"

        # Final chunk finish reason
        assert chunks[-1]["choices"][0]["finish_reason"] == "tool_calls"
    finally:
        gateway.close()
        upstream.close()


def test_legacy_functions_and_parallel_tool_calls():
    body = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "hello"}],
        "functions": [{
            "name": "lookup",
            "description": "lookup user",
            "parameters": {"type": "object", "properties": {"id": {"type": "string"}}},
        }],
        "parallel_tool_calls": False,
    }
    result = translate_request(body)
    assert result["parallel_tool_calls"] is False
    assert result["tools"] == [{
        "type": "function",
        "name": "lookup",
        "description": "lookup user",
        "parameters": {"type": "object", "properties": {"id": {"type": "string"}}},
    }]


def test_translate_assistant_with_content_and_tool_calls():
    messages = [
        {
            "role": "assistant",
            "content": "Looking up details...",
            "tool_calls": [{
                "id": "c1",
                "type": "function",
                "function": {"name": "query", "arguments": "{}"},
            }],
        },
        {"role": "tool", "tool_call_id": "c1", "content": "found"},
    ]
    instructions, items = translate_messages(messages)
    assert len(items) == 3
    assert items[0] == {"role": "assistant", "content": [{"type": "output_text", "text": "Looking up details..."}]}
    assert items[1] == {"type": "function_call", "call_id": "c1", "name": "query", "arguments": "{}"}
    assert items[2] == {"type": "function_call_output", "call_id": "c1", "output": "found"}


def test_translate_request_extracts_reasoning_effort_from_body():
    # Via reasoning_effort field
    body1 = {
        "model": "grok-4.7",
        "reasoning_effort": "low",
        "messages": [{"role": "user", "content": "hi"}],
    }
    result1 = translate_request(body1)
    assert result1["reasoning"] == {"effort": "low"}
    assert result1["model"] == "grok-4.7"

    # Via reasoning object
    body2 = {
        "model": "grok-4.7",
        "reasoning": {"effort": "HIGH"},
        "messages": [{"role": "user", "content": "hi"}],
    }
    result2 = translate_request(body2)
    assert result2["reasoning"] == {"effort": "high"}

    # Via model suffix (-low, -medium)
    body3 = {
        "model": "grok-4.7-medium",
        "messages": [{"role": "user", "content": "hi"}],
    }
    result3 = translate_request(body3)
    assert result3["reasoning"] == {"effort": "medium"}
    assert result3["model"] == "grok-4.7"

    # With router/provider prefix
    body4 = {
        "model": "ggw/grok-4.7-high",
        "messages": [{"role": "user", "content": "hi"}],
    }
    result4 = translate_request(body4)
    assert result4["reasoning"] == {"effort": "high"}
    assert result4["model"] == "grok-4.7"

    # Generic grok name defaults to grok-4.7
    body5 = {
        "model": "ggw/grok",
        "messages": [{"role": "user", "content": "hi"}],
    }
    result5 = translate_request(body5)
    assert result5["model"] == "grok-4.7"

    # Unsupported efforts are normalized to prevent upstream HTTP 400
    body_none = {
        "model": "grok-4.7",
        "reasoning_effort": "none",
        "messages": [{"role": "user", "content": "hi"}],
    }
    assert translate_request(body_none)["reasoning"] == {"effort": "minimal"}

    body_max = {
        "model": "grok-4.7",
        "reasoning_effort": "max",
        "messages": [{"role": "user", "content": "hi"}],
    }
    assert translate_request(body_max)["reasoning"] == {"effort": "xhigh"}


def test_translate_messages_handles_assistant_with_reasoning_content():
    messages = [
        {"role": "user", "content": "question"},
        {"role": "assistant", "reasoning_content": "thinking", "content": "answer"},
        {"role": "assistant", "content": [
            {"type": "thinking", "thinking": "scratchpad"},
            {"type": "text", "text": "final reply"},
        ]},
        {"role": "user", "content": "followup"},
    ]
    instructions, items = translate_messages(messages)
    assert len(items) == 4
    assert items[1] == {"role": "assistant", "content": [{"type": "output_text", "text": "answer"}]}
    assert items[2] == {"role": "assistant", "content": [{"type": "output_text", "text": "final reply"}]}


def test_models_endpoint_lists_supported_models(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        # /v1/models with auth
        conn = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        conn.request("GET", "/v1/models", headers={"Authorization": "Bearer gateway-secret"})
        res = conn.getresponse()
        assert res.status == 200
        data = json.loads(res.read().decode())
        assert data["object"] == "list"
        model_ids = [m["id"] for m in data["data"]]
        assert "grok-4.7" in model_ids
        assert "grok-4.7-high" in model_ids
        conn.close()

        # /models alias with auth
        conn2 = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        conn2.request("GET", "/models", headers={"Authorization": "Bearer gateway-secret"})
        res2 = conn2.getresponse()
        assert res2.status == 200
        conn2.close()

        # /v1/models without auth
        conn3 = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        conn3.request("GET", "/v1/models")
        res3 = conn3.getresponse()
        assert res3.status == 401
        conn3.close()
    finally:
        gateway.close()
        upstream.close()


def test_cors_options_returns_allowed_headers(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        conn.request("OPTIONS", "/v1/chat/completions")
        res = conn.getresponse()
        assert res.status == 204
        assert res.getheader("Access-Control-Allow-Origin") == "*"
        assert "POST" in (res.getheader("Access-Control-Allow-Methods") or "")
        conn.close()
    finally:
        gateway.close()
        upstream.close()


def test_health_check_requires_no_auth(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        for path in ("/health", "/healthz", "/"):
            conn = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
            conn.request("GET", path)
            res = conn.getresponse()
            assert res.status == 200
            data = json.loads(res.read().decode())
            assert data["status"] == "ok"
            conn.close()
    finally:
        gateway.close()
        upstream.close()


def test_chat_completions_route_alias(store):
    upstream = FakeUpstream(lambda count, payload: (200, "".join(text_events())))
    gateway = RunningGateway(store, upstream.url)
    try:
        conn = http.client.HTTPConnection("127.0.0.1", gateway.port, timeout=5)
        body = json.dumps(chat())
        conn.request("POST", "/chat/completions", body=body, headers={
            "Authorization": "Bearer gateway-secret",
            "Content-Type": "application/json",
        })
        res = conn.getresponse()
        assert res.status == 200
        data = json.loads(res.read().decode())
        assert data["choices"][0]["message"]["content"] == "ok"
        conn.close()
    finally:
        gateway.close()
        upstream.close()


def test_response_incomplete_sets_finish_reason_length(store):
    events = [
        sse({"type": "response.output_text.delta", "delta": "cut off"}),
        sse({"type": "response.incomplete", "response": {"usage": {"total_tokens": 10}}}),
        "data: [DONE]\n\n",
    ]
    upstream = FakeUpstream(lambda count, payload: (200, "".join(events)))
    gateway = RunningGateway(store, upstream.url)
    try:
        status, raw = gateway.post(chat(stream=True))
        assert status == 200
        lines = [line[6:] for line in raw.splitlines() if line.startswith("data: ")]
        assert lines[-1] == "[DONE]"
        chunks = [json.loads(line) for line in lines[:-1]]
        assert chunks[-1]["choices"][0]["finish_reason"] == "length"
    finally:
        gateway.close()
        upstream.close()


def test_reasoning_cache_lru_and_lookups():
    cache = ReasoningCache(max_entries=2)
    item1 = {"type": "reasoning", "summary": [], "encrypted_content": "enc1"}
    item2 = {"type": "reasoning", "summary": [], "encrypted_content": "enc2"}
    item3 = {"type": "reasoning", "summary": [], "encrypted_content": "enc3"}

    cache.put("call1", item1)
    cache.put("call2", item2)
    res1 = cache.get("call1")
    assert res1 is not None and res1["encrypted_content"] == "enc1"

    # Eviction: adding item3 evicts call2 because call1 was accessed
    cache.put("call3", item3)
    assert cache.get("call1") is not None
    assert cache.get("call2") is None
    assert cache.get("call3") is not None

    # By text lookup
    cache.put_by_text("hello assistant", item1)
    res_text = cache.get_by_text("hello assistant")
    assert res_text is not None and res_text["encrypted_content"] == "enc1"
    assert cache.get_by_text("other text") is None


def test_responses_translator_captures_reasoning_item():
    translator = ResponsesTranslator()
    translator.feed({"type": "response.output_item.added", "item": {
        "id": "rs_1", "type": "reasoning", "status": "in_progress", "summary": []
    }})
    translator.feed({"type": "response.output_item.done", "item": {
        "id": "rs_1", "type": "reasoning", "status": "completed",
        "summary": [], "encrypted_content": "secret_blob",
    }})
    item = translator.get_reasoning_item()
    assert item is not None
    assert item["encrypted_content"] == "secret_blob"
    assert item["type"] == "reasoning"


def test_translate_messages_stitches_cached_reasoning():
    cache = ReasoningCache()
    cache.put("call_abc", {"type": "reasoning", "summary": [], "encrypted_content": "cipher_xyz"})
    messages = [
        {"role": "user", "content": "do task"},
        {"role": "assistant", "content": None, "tool_calls": [{
            "id": "call_abc",
            "type": "function",
            "function": {"name": "terminal", "arguments": "{}"},
        }]},
        {"role": "tool", "tool_call_id": "call_abc", "content": "result"},
    ]
    _, items = translate_messages(messages, reasoning_cache=cache)
    assert len(items) == 4
    assert items[0] == {"role": "user", "content": [{"type": "input_text", "text": "do task"}]}
    assert items[1] == {"type": "reasoning", "summary": [], "encrypted_content": "cipher_xyz"}
    assert items[2]["type"] == "function_call"
    assert items[2]["call_id"] == "call_abc"
    assert items[3]["type"] == "function_call_output"


def test_end_to_end_reasoning_stitching_multi_turn(store):
    recorded_payloads = []

    def handle(count, payload):
        recorded_payloads.append(payload)
        if count == 1:
            events = [
                sse({"type": "response.output_item.added", "item": {
                    "id": "rs_t1", "type": "reasoning", "status": "in_progress", "summary": []
                }}),
                sse({"type": "response.output_item.done", "item": {
                    "id": "rs_t1", "type": "reasoning", "status": "completed",
                    "summary": [], "encrypted_content": "e2e_trace_blob"
                }}),
                sse({"type": "response.output_item.added", "output_index": 1, "item": {
                    "type": "function_call", "id": "fc_1", "call_id": "call_99", "name": "read_file"
                }}),
                sse({"type": "response.function_call_arguments.done", "output_index": 1, "item_id": "fc_1"}),
                sse({"type": "response.output_item.done", "output_index": 1, "item": {
                    "type": "function_call", "id": "fc_1", "call_id": "call_99", "name": "read_file",
                    "arguments": "{\"path\": \"package.json\"}"
                }}),
                sse({"type": "response.completed", "response": {}}),
                "data: [DONE]\n\n",
            ]
            return 200, "".join(events)
        else:
            events = [
                sse({"type": "response.output_text.delta", "delta": "All done"}),
                sse({"type": "response.completed", "response": {}}),
                "data: [DONE]\n\n",
            ]
            return 200, "".join(events)

    upstream = FakeUpstream(handle)
    gateway = RunningGateway(store, upstream.url)
    try:
        # Turn 1: Client sends initial user message
        req1 = {
            "model": "grok-4.7",
            "messages": [{"role": "user", "content": "check dependencies"}],
            "stream": True,
        }
        status1, raw1 = gateway.post(req1)
        assert status1 == 200
        assert "call_99" in raw1

        # Turn 2: Client sends conversation history including the tool call result
        req2 = {
            "model": "grok-4.7",
            "messages": [
                {"role": "user", "content": "check dependencies"},
                {"role": "assistant", "content": None, "tool_calls": [{
                    "id": "call_99", "type": "function",
                    "function": {"name": "read_file", "arguments": "{\"path\": \"package.json\"}"}
                }]},
                {"role": "tool", "tool_call_id": "call_99", "content": "{}"},
            ],
            "stream": True,
        }
        status2, raw2 = gateway.post(req2)
        assert status2 == 200

        # Assert Turn 2 payload delivered to upstream has the stitched reasoning item!
        assert len(recorded_payloads) == 2
        turn2_payload = recorded_payloads[1]
        turn2_input = turn2_payload["input"]
        # input[0] is user, input[1] is stitched reasoning, input[2] is function_call, input[3] is function_call_output
        assert turn2_input[0]["role"] == "user"
        assert turn2_input[1]["type"] == "reasoning"
        assert turn2_input[1]["encrypted_content"] == "e2e_trace_blob"
        assert turn2_input[2]["type"] == "function_call"
        assert turn2_input[3]["type"] == "function_call_output"
    finally:
        gateway.close()
        upstream.close()


def test_translate_request_response_format_json_and_schema():
    body_json = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "give json"}],
        "response_format": {"type": "json_object"},
    }
    r1 = translate_request(body_json)
    assert r1["text"] == {"format": {"type": "json_object"}}

    body_schema = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "give structured"}],
        "response_format": {
            "type": "json_schema",
            "json_schema": {"name": "user", "schema": {"type": "object"}},
        },
    }
    r2 = translate_request(body_schema)
    assert r2["text"] == {"format": {"type": "json_schema", "schema": {"type": "object"}}}


def test_translate_request_tool_choice_normalization():
    # When tools are provided, nested function choice is flattened
    body = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "calc"}],
        "tools": [{"type": "function", "function": {"name": "calculator"}}],
        "tool_choice": {"type": "function", "function": {"name": "calculator"}},
    }
    r1 = translate_request(body)
    assert r1["tool_choice"] == {"type": "function", "name": "calculator"}

    # When no tools are provided, tool_choice is dropped to avoid xAI HTTP 400
    body_no_tools = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "hi"}],
        "tool_choice": "auto",
    }
    r2 = translate_request(body_no_tools)
    assert "tool_choice" not in r2


def test_translate_messages_multimodal_vision():
    messages = [
        {
            "role": "user",
            "content": [
                {"type": "text", "text": "what is this?"},
                {"type": "image_url", "image_url": {"url": "https://example.com/cat.png"}},
            ],
        }
    ]
    _, items = translate_messages(messages)
    assert len(items) == 1
    content = items[0]["content"]
    assert len(content) == 2
    assert content[0] == {"type": "input_text", "text": "what is this?"}
    assert content[1] == {"type": "input_image", "image_url": "https://example.com/cat.png"}


def test_translate_request_sampling_and_metadata():
    body = {
        "model": "grok-4.7",
        "messages": [{"role": "user", "content": "hi"}],
        "temperature": 0.7,
        "top_p": 0.9,
        "presence_penalty": 0.2,
        "frequency_penalty": 0.3,
        "seed": 42,
        "stop": ["STOP"],
        "user": "client-uid",
        "metadata": {"some": "framework-leak"},  # should be omitted
    }
    res = translate_request(body)
    assert res["temperature"] == 0.7
    assert res["top_p"] == 0.9
    assert res["presence_penalty"] == 0.2
    assert res["frequency_penalty"] == 0.3
    assert res["seed"] == 42
    assert res["stop"] == ["STOP"]
    assert res["user"] == "client-uid"
    assert "metadata" not in res
    assert res["store"] is False







