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
    Account,
    AccountStore,
    NoAccountAvailable,
    ProtocolError,
    ResponsesTranslator,
    load_accounts,
    make_server,
    make_tls_context,
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


def test_corrupt_state_is_preserved_without_reading_contents(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{ definitely-not-json")
    recovered = AccountStore([account("a")], path, clock=lambda: 1.0)
    assert recovered.select().id == "a"
    backups = list(tmp_path.glob("state.json.corrupt-*"))
    assert len(backups) == 1
    assert backups[0].read_text() == "{ definitely-not-json"


def test_inflight_accounts_are_never_shared(store):
    barrier = threading.Barrier(3)
    selected = []

    def choose():
        chosen = store.select()
        selected.append(chosen.id)
        barrier.wait(timeout=2)
        store.release(chosen.id)

    threads = [threading.Thread(target=choose) for _ in range(3)]
    for thread in threads:
        thread.start()
    barrier.wait(timeout=2)
    assert sorted(selected) == ["a", "b", "c"]
    with pytest.raises(NoAccountAvailable) as error:
        store.select()
    assert error.value.kind == "busy"
    for thread in threads:
        thread.join(timeout=2)


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
