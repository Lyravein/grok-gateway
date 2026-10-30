"""OpenAI-compatible gateway for Grok CLI OAuth accounts.

See plan.md for the specification. Python 3.10+ standard library only.
"""

import argparse
import hmac
import http.client
import json
import logging
import os
import sqlite3
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("grok-gateway")

EXHAUSTED_CODES = ("subscription:free-usage-exhausted", "free-usage-exhausted")
LOCK_SECONDS = 24 * 3600
COOLDOWN_SECONDS = 30
RESET_KEYS = ("resets_at", "reset_at", "reset_time", "resets", "resetsAt")


# --------------------------------------------------------------------------
# Accounts
# --------------------------------------------------------------------------

@dataclass(frozen=True)
class Account:
    id: str
    access_token: str = field(repr=False)


def _open_readonly(path):
    uri = "file:%s?mode=ro" % urllib.parse.quote(os.path.abspath(str(path)))
    return sqlite3.connect(uri, uri=True)


def load_accounts(db_path):
    try:
        db = _open_readonly(db_path)
    except sqlite3.Error:
        log.error("9Router database unavailable")
        return []
    try:
        rows = db.execute(
            "SELECT id, data FROM providerConnections"
            " WHERE provider='grok-cli' AND isActive=1"
        ).fetchall()
    except sqlite3.Error:
        log.error("9Router database query failed")
        return []
    finally:
        db.close()
    accounts, skipped = [], 0
    for row_id, data in rows:
        try:
            token = json.loads(data)["accessToken"]
        except (TypeError, ValueError, KeyError):
            skipped += 1
            continue
        if not isinstance(token, str) or not token:
            skipped += 1
            continue
        accounts.append(Account(id=str(row_id), access_token=token))
    if skipped:
        log.warning("skipped %d unusable grok-cli rows", skipped)
    return accounts


class NoAccountAvailable(Exception):
    def __init__(self, kind, retry_at=None):
        super().__init__(kind)
        self.kind = kind  # "exhausted" or "busy"
        self.retry_at = retry_at


class ProtocolError(Exception):
    """Upstream SSE could not be converted without fabricating output."""


# --------------------------------------------------------------------------
# Account store: eligibility, rotation, exhaustion, persistence
# --------------------------------------------------------------------------

class AccountStore:
    def __init__(self, accounts, state_path, clock=time.time, source=None):
        self.state_path = str(state_path)
        self.clock = clock
        self.source = source
        self._lock = threading.RLock()
        self._free_reset = {}
        self._pending = {}
        with self._lock:
            self._state = self._load_state()
            self.sync(accounts)

    # -- persistence ------------------------------------------------------

    def _load_state(self):
        state = {"version": 1, "accounts": {}}
        try:
            with open(self.state_path, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
        except FileNotFoundError:
            return state
        except (OSError, ValueError):
            backup = "%s.corrupt-%d" % (self.state_path, int(self.clock()))
            try:
                os.replace(self.state_path, backup)
            except OSError:
                pass
            log.error("state file corrupt; preserved it and started empty")
            return state
        accounts = loaded.get("accounts") if isinstance(loaded, dict) else None
        if not isinstance(accounts, dict):
            log.error("state file malformed; starting empty")
            return state
        state["accounts"] = {
            str(k): v for k, v in accounts.items() if isinstance(v, dict)
        }
        return state

    def _persist(self):
        data = {"version": 1, "accounts": self._state["accounts"]}
        directory = os.path.dirname(os.path.abspath(self.state_path))
        os.makedirs(directory, mode=0o700, exist_ok=True)
        tmp = "%s.tmp-%d-%d" % (self.state_path, os.getpid(), threading.get_ident())
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def _entry(self, account_id):
        return self._state["accounts"].setdefault(account_id, {})

    def check_permissions(self):
        """Refuse to run with state or its directory accessible to others."""
        directory = os.path.dirname(os.path.abspath(self.state_path))
        try:
            directory_mode = os.stat(directory).st_mode
        except FileNotFoundError:
            directory_mode = 0o700
        if directory_mode & 0o077:
            raise RuntimeError("state directory is group/world accessible")
        try:
            mode = os.stat(self.state_path).st_mode
        except FileNotFoundError:
            return
        if mode & 0o077:
            raise RuntimeError("state file is group/world accessible")

    # -- account set ------------------------------------------------------

    def sync(self, accounts):
        """Replace the known credential set, keeping persisted state."""
        with self._lock:
            self._accounts = {a.id: a for a in accounts}
            self._pending = {
                k: v for k, v in self._pending.items() if k in self._accounts
            }

    # -- selection --------------------------------------------------------

    def select(self, exclude=(), touch=True):
        """Reserve the least-recently-used eligible account.

        ``touch=False`` is used for fallback attempts: a retry must not
        reorder rotation, otherwise the fallback account would jump ahead
        of accounts that were never tried.
        """
        with self._lock:
            now = self.clock()
            candidates = [
                a for a in self._accounts
                if a not in self._pending and a not in exclude
                and self._entry(a).get("exhausted_until", 0) <= now
                and self._entry(a).get("cooldown_until", 0) <= now
            ]
            if not candidates:
                future = [
                    self._entry(a)["exhausted_until"]
                    for a in self._accounts if a not in self._pending and a not in exclude
                    and "exhausted_until" in self._entry(a)
                ]
                if future:
                    raise NoAccountAvailable("exhausted", min(future))
                raise NoAccountAvailable("busy")
            account_id = min(candidates, key=lambda a: self._entry(a).get("last_used", 0))
            self._pending[account_id] = True
            if touch:
                self._entry(account_id)["last_used"] = now
            return self._accounts[account_id]

    def release(self, account_id):
        with self._lock:
            if self._pending.pop(account_id, None) is not None:
                self._persist()

    # -- failures ---------------------------------------------------------

    def _save_free_reset(self, account_id, body):
        value = self._find_reset(body)
        if value is not None:
            self._free_reset[account_id] = value

    @staticmethod
    def _find_reset(body):
        try:
            obj = json.loads(body)
        except (TypeError, ValueError):
            return None
        stack = [obj]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in RESET_KEYS and isinstance(value, (int, float, str)):
                        if isinstance(value, str):
                            try:
                                value = float(value)
                            except ValueError:
                                continue
                        if isinstance(value, (int, float)):
                            value = float(value)
                            if 1_000_000_000 <= value < 10_000_000_000:
                                return value
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node)
        return None

    def mark_exhausted(self, account_id, body=None, reset_at=None):
        with self._lock:
            if reset_at is None:
                if body is not None:
                    self._save_free_reset(account_id, body)
                reset_at = self._free_reset.pop(account_id, None)
            now = self.clock()
            lock_until = reset_at if reset_at and reset_at > now else now + LOCK_SECONDS
            entry = self._entry(account_id)
            if lock_until > entry.get("exhausted_until", 0):
                entry["exhausted_until"] = lock_until
                entry["exhaustions"] = entry.get("exhaustions", 0) + 1
                self._persist()
            return lock_until

    def cooldown(self, account_id, seconds=COOLDOWN_SECONDS):
        with self._lock:
            self._entry(account_id)["cooldown_until"] = self.clock() + seconds
            self._persist()


def contains_exhaustion(text):
    low = text.lower()
    return any(code in low for code in EXHAUSTED_CODES)


# --------------------------------------------------------------------------
# Request conversion: [OI] chat completions -> Responses API
# --------------------------------------------------------------------------

def text_of(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, str):
                parts.append(part)
            elif isinstance(part, dict) and part.get("type") in (None, "text", "input_text", "output_text"):
                if isinstance(part.get("text"), str):
                    parts.append(part["text"])
        return "".join(parts)
    return ""


def translate_messages(messages):
    instructions, items = [], []
    for message in messages or []:
        role = message.get("role")
        text = text_of(message.get("content"))
        if role in ("system", "developer"):
            if text:
                instructions.append(text)
        elif role == "user":
            items.append({"role": "user", "content": [{"type": "input_text", "text": text}]})
        elif role == "assistant":
            if text:
                items.append({"role": "assistant", "content": [{"type": "output_text", "text": text}]})
            for call in message.get("tool_calls") or []:
                function = call.get("function") or {}
                items.append({
                    "type": "function_call",
                    "call_id": call.get("id"),
                    "name": function.get("name"),
                    "arguments": function.get("arguments") or "{}",
                })
        elif role == "tool":
            items.append({
                "type": "function_call_output",
                "call_id": message.get("tool_call_id"),
                "output": text,
            })
    return "\n\n".join(instructions), items


def translate_request(body):
    raw_model = str(body.get("model") or "grok-4.7").strip()
    model = raw_model.split("/")[-1] if "/" in raw_model else raw_model
    effort = None
    if isinstance(body.get("reasoning_effort"), str):
        effort = body["reasoning_effort"].lower()
    elif isinstance(body.get("reasoning"), dict) and isinstance(body["reasoning"].get("effort"), str):
        effort = body["reasoning"]["effort"].lower()

    if not effort:
        for suffix in ("-xhigh", "-high", "-medium", "-low"):
            if model.endswith(suffix):
                model, effort = model[: -len(suffix)], suffix[1:]
                break
    if model in ("", "grok"):
        model = "grok-4.7"
    instructions, items = translate_messages(body.get("messages"))
    result = {"model": model, "input": items, "stream": True}
    if instructions:
        result["instructions"] = instructions
    if "max_tokens" in body:
        result["max_output_tokens"] = body["max_tokens"]
    elif "max_completion_tokens" in body:
        result["max_output_tokens"] = body["max_completion_tokens"]
    if effort:
        result["reasoning"] = {"effort": effort}
    tools = [
        {"type": "function", **t["function"]} for t in body.get("tools") or []
        if isinstance(t, dict) and t.get("type") == "function" and isinstance(t.get("function"), dict)
    ]
    if not tools and isinstance(body.get("functions"), list):
        for f in body["functions"]:
            if isinstance(f, dict) and "name" in f:
                tools.append({"type": "function", **f})
    if tools:
        result["tools"] = tools
    if "tool_choice" in body:
        result["tool_choice"] = body["tool_choice"]
    if "parallel_tool_calls" in body:
        result["parallel_tool_calls"] = bool(body["parallel_tool_calls"])
    return result


# --------------------------------------------------------------------------
# Responses SSE -> [OI] chunk translation
# --------------------------------------------------------------------------

class ResponsesTranslator:
    """Converts Responses SSE events into [OI] chat deltas.

    ``calls`` entries look like ``{"index", "id", "name", "arguments", ...}``.
    """

    def __init__(self):
        self.calls = []
        self.content = ""
        self.reasoning = ""
        self.usage = None
        self._by_index, self._by_id = {}, {}
        self._delta_text = set()
        self._delta_reasoning = set()
        self._complete = set()

    # -- helpers ----------------------------------------------------------

    def _resolve(self, output_index, item_id):
        by_index = self._by_index.get(output_index)
        by_id = self._by_id.get(item_id) if item_id is not None else None
        if by_index is not None and by_id is not None and by_index is not by_id:
            raise ProtocolError("conflicting tool call identity")
        return by_index if by_index is not None else by_id

    def _new_call(self, output_index, item_id, call_id, name):
        call = {
            "index": len(self.calls), "item_id": item_id, "output_index": output_index,
            "id": call_id or item_id, "name": name, "arguments": "",
            "has_delta": False,
        }
        self.calls.append(call)
        if output_index is not None:
            self._by_index[output_index] = call
        if item_id is not None:
            self._by_id[item_id] = call
        return call

    # -- events -----------------------------------------------------------

    def feed(self, event):
        handler = getattr(self, "_on_" + str(event.get("type")).replace(".", "_"), None)
        return handler(event) if handler else []

    def _on_response_output_text_delta(self, event):
        delta = event.get("delta")
        if not isinstance(delta, str):
            raise ProtocolError("text delta missing")
        self._delta_text.add(event.get("output_index", event.get("item_id")))
        self.content += delta
        return [{"delta": {"content": delta}}]

    def _on_response_output_text_done(self, event):
        index = event.get("output_index", event.get("item_id"))
        text = event.get("text")
        if index in self._delta_text or not isinstance(text, str) or not text:
            return []
        self.content += text
        return [{"delta": {"content": text}}]

    def _on_response_reasoning_text_delta(self, event):
        delta = event.get("delta")
        if not isinstance(delta, str):
            raise ProtocolError("reasoning delta missing")
        self._delta_reasoning.add(event.get("output_index", event.get("item_id")))
        self.reasoning += delta
        return [{"delta": {"reasoning_content": delta}}]

    def _on_response_reasoning_summary_text_delta(self, event):
        return self._on_response_reasoning_text_delta(event)

    def _on_response_reasoning_text_done(self, event):
        index = event.get("output_index", event.get("item_id"))
        text = event.get("text")
        if index in self._delta_reasoning or not isinstance(text, str) or not text:
            return []
        self.reasoning += text
        return [{"delta": {"reasoning_content": text}}]

    def _on_response_reasoning_summary_text_done(self, event):
        return self._on_response_reasoning_text_done(event)

    def _open_call_packets(self, call):
        if call.get("opened"):
            return []
        call["opened"] = True
        return [{"delta": {"tool_calls": [{
            "index": call["index"],
            "id": call["id"],
            "type": "function",
            "function": {"name": call["name"], "arguments": ""},
        }]}}]

    def _on_response_output_item_added(self, event):
        item = event.get("item") or {}
        if item.get("type") != "function_call":
            return []
        if not item.get("name"):
            raise ProtocolError("tool call without a name")
        self._new_call(event.get("output_index"), item.get("id"),
                       item.get("call_id"), item["name"])
        return []

    def _on_response_function_call_arguments_delta(self, event):
        delta = event.get("delta")
        if not isinstance(delta, str):
            raise ProtocolError("argument delta missing")
        call = self._resolve(event.get("output_index"), event.get("item_id"))
        if call is None:
            raise ProtocolError("unknown tool call reference")
        call["arguments"] += delta
        call["has_delta"] = True
        packets = self._open_call_packets(call)
        packets.append({"delta": {"tool_calls": [
            {"index": call["index"], "function": {"arguments": delta}}]}})
        return packets

    def _on_response_output_item_done(self, event):
        item = event.get("item") or {}
        if item.get("type") not in (None, "function_call"):
            return []
        output_index, item_id = event.get("output_index"), item.get("id")
        call = self._resolve(output_index, item_id)
        if call is None:
            if not item.get("name"):
                raise ProtocolError("unknown tool call reference")
            call = self._new_call(output_index, item_id, item.get("call_id"), item["name"])
        elif item_id is not None and call["item_id"] is not None and item_id != call["item_id"]:
            raise ProtocolError("conflicting tool call identity")
        arguments = item.get("arguments")
        if not call["has_delta"] and isinstance(arguments, str):
            call["arguments"] = arguments
        if not call["id"]:
            call["id"] = item.get("call_id") or call["item_id"]
        if not call["id"] or not call["name"]:
            raise ProtocolError("tool call without identity")
        try:
            json.loads(call["arguments"])
        except ValueError:
            raise ProtocolError("tool arguments are not valid JSON")
        self._complete.add(call["index"])
        packets = self._open_call_packets(call)
        if not call["has_delta"]:
            call["has_delta"] = True
            packets.append({"delta": {"tool_calls": [
                {"index": call["index"], "function": {"arguments": call["arguments"]}}]}})
        return packets

    def _on_response_completed(self, event):
        response = event.get("response") or {}
        if isinstance(response.get("usage"), dict):
            self.usage = response["usage"]
        return []

    # -- terminal ---------------------------------------------------------

    def opening_tool_packets(self):
        return [{"delta": {"tool_calls": [{
            "index": call["index"], "id": call["id"], "type": "function",
            "function": {"name": call["name"], "arguments": ""},
        }]}} for call in self.calls]

    def finish(self):
        if len(self._complete) != len(self.calls):
            raise ProtocolError("incomplete tool call")
        return ("tool_calls" if self.calls else "stop"), self.usage


# --------------------------------------------------------------------------
# TLS
# --------------------------------------------------------------------------

def make_tls_context():
    context = ssl.create_default_context()
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


# --------------------------------------------------------------------------
# HTTP gateway
# --------------------------------------------------------------------------

class GatewayState:
    def __init__(self, store, upstream_url, api_key, timeout, max_attempts, max_body_bytes):
        self.store = store
        self.upstream_url = upstream_url
        self.api_key = api_key
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.max_body_bytes = max_body_bytes
        self.tls = make_tls_context()
        self.db_path = getattr(store, "source", None)
        self._reload_lock = threading.Lock()
        self._last_load = 0.0

    def reload(self):
        """Re-read credentials from 9Router at most once every 30 seconds."""
        if not self.db_path:
            return
        with self._reload_lock:
            now = time.time()
            if now - self._last_load < 30:
                return
            self._last_load = now
        self.store.sync(load_accounts(self.db_path))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


SUPPORTED_MODELS = [
    "grok-4.7",
    "grok-4.7-high",
    "grok-4.7-xhigh",
    "grok-4.7-medium",
    "grok-4.7-low",
    "grok-4.6",
    "grok-4.6-high",
    "grok-4.5",
]

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type, Accept, User-Agent, X-Requested-With",
    "Access-Control-Max-Age": "86400",
}


def _json_response(handler, status, payload, headers=None):
    raw = json.dumps(payload).encode()
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(raw)))
    merged = dict(CORS_HEADERS)
    if headers:
        merged.update(headers)
    for key, value in merged.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(raw)


def _error(handler, status, message, code="error", headers=None):
    _json_response(handler, status, {"error": {"message": message, "type": code}}, headers)


def _chunk(choices=None, model="grok", **extra):
    payload = {"id": "chatcmpl-gateway", "object": "chat.completion.chunk",
               "created": int(time.time()), "model": model, "choices": choices or []}
    payload.update(extra)
    return payload


def openai_usage(usage):
    """Map Responses token names onto [OI] chat completion names."""
    if not isinstance(usage, dict):
        return None
    result = {
        "prompt_tokens": usage.get("input_tokens", usage.get("prompt_tokens", 0)),
        "completion_tokens": usage.get("output_tokens", usage.get("completion_tokens", 0)),
        "total_tokens": usage.get("total_tokens", 0),
    }
    output_details = usage.get("output_tokens_details") or {}
    reasoning_tokens = usage.get("reasoning_tokens", output_details.get("reasoning_tokens"))
    if reasoning_tokens is not None:
        result["completion_tokens_details"] = {"reasoning_tokens": int(reasoning_tokens)}
    return result


class GatewayRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    state = None  # type: GatewayState

    # http.server logs to stderr; keep it quiet and generic.
    def log_message(self, fmt, *args):
        log.debug("http request handled")

    def _authorized(self):
        header = self.headers.get("Authorization", "")
        expected = "Bearer " + self.state.api_key
        return hmac.compare_digest(header.encode(), expected.encode())

    def do_OPTIONS(self):
        self.send_response(204)
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self):
        path = self.path.split("?")[0].rstrip("/")
        if path in ("", "/health", "/healthz"):
            return _json_response(self, 200, {"status": "ok"})
        if path in ("/models", "/v1/models"):
            if not self._authorized():
                self.close_connection = True
                return _error(self, 401, "unauthorized", "unauthorized")
            data = [
                {
                    "id": m,
                    "object": "model",
                    "created": 1700000000,
                    "owned_by": "xai",
                    "permission": [],
                    "root": m,
                    "parent": None,
                }
                for m in SUPPORTED_MODELS
            ]
            return _json_response(self, 200, {"object": "list", "data": data})
        return _error(self, 404, "not found", "not_found")

    def do_POST(self):
        path = self.path.split("?")[0].rstrip("/")
        if path not in ("/v1/chat/completions", "/chat/completions"):
            return _error(self, 404, "not found", "not_found")
        if not self._authorized():
            self.close_connection = True
            return _error(self, 401, "unauthorized", "unauthorized")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            return _error(self, 400, "bad request", "invalid_request")
        if length < 0:
            self.close_connection = True
            return _error(self, 400, "bad request", "invalid_request")
        if length > self.state.max_body_bytes:
            self.close_connection = True
            return _error(self, 413, "request too large", "request_too_large")
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw)
            if not isinstance(body, dict):
                raise ValueError
        except ValueError:
            return _error(self, 400, "invalid JSON body", "invalid_request")
        self.state.reload()
        Context(self.state, body).run(self)


class Context:
    def __init__(self, state, body):
        self.state = state
        self.body = body
        self.request_id = os.urandom(6).hex()
        self.tried = set()
        self.stream = bool(body.get("stream"))
        self._stream_started = False
        self.model = body.get("model") or "grok"

    # -- entry ------------------------------------------------------------

    def run(self, handler):
        self.handler = handler
        try:
            self.upstream_body = translate_request(self.body)
        except Exception:
            log.error("request %s: bad request body", self.request_id)
            return _error(handler, 400, "invalid request", "invalid_request")

        started = time.monotonic()
        for attempt in range(1, self.state.max_attempts + 1):
            account = self._select(not self.tried)
            if isinstance(account, NoAccountAvailable):
                return self._reject(account)
            self.tried.add(account.id)
            try:
                outcome = self._attempt(attempt, account)
            finally:
                self.state.store.release(account.id)
            log.info("request %s attempt %d model %s outcome %s elapsed %.2fs",
                     self.request_id, attempt, self.model, outcome,
                     time.monotonic() - started)
            if outcome != "retry":
                return
        _error(handler, 429, "upstream unavailable", "rate_limited",
               {"Retry-After": str(COOLDOWN_SECONDS)})

    def _select(self, touching):
        try:
            return self.state.store.select(exclude=self.tried, touch=touching)
        except NoAccountAvailable as error:
            return error

    def _reject(self, error):
        if error.kind == "exhausted":
            reset = max(1, int(error.retry_at - time.time())) if error.retry_at else LOCK_SECONDS
            _error(self.handler, 429, "all accounts exhausted", "rate_limited",
                   {"Retry-After": str(reset)})
        else:
            _error(self.handler, 503, "all accounts busy", "busy", {"Retry-After": "1"})

    # -- upstream ---------------------------------------------------------

    def _open(self, account):
        request = urllib.request.Request(
            self.state.upstream_url,
            data=json.dumps(self.upstream_body).encode(),
            headers={
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
                "Authorization": "Bearer " + account.access_token,
                "User-Agent": "grok-shell/1.0.44 (linux; x86_64)",
                "x-grok-client-identifier": "grok-shell",
                "x-grok-client-version": "1.0.44",
                "x-xai-token-auth": "xai-grok-cli",
                "x-grok-client-mode": "headless",
            },
            method="POST",
        )
        return urllib.request.build_opener(urllib.request.HTTPSHandler(
            context=self.state.tls), _NoRedirect()).open(request, timeout=self.state.timeout)

    def _attempt(self, attempt, account):
        """Returns done | retry | fatal | protocol."""
        try:
            response = self._open(account)
        except urllib.error.HTTPError as error:
            body = self._read_body(error)
            return self._handle_failure(account, error.code, body)
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            log.warning("request %s attempt %d: upstream connection failed",
                        self.request_id, attempt)
            self.state.store.cooldown(account.id)
            return "retry"

        try:
            translator = ResponsesTranslator()
            buffered = []
            started = False
            output_accepted = False
            ended = False
            try:
                for raw_line in response:
                    line = raw_line.decode("utf-8", "replace").strip()
                    if not line.startswith("data:"):
                        continue
                    data = line[5:].strip()
                    if data == "[DONE]":
                        ended = True
                        break
                    try:
                        event = json.loads(data)
                        if not isinstance(event, dict):
                            raise ValueError
                    except ValueError:
                        raise ProtocolError("invalid event data")
                    kind = event.get("type")
                    if kind in ("error", "response.failed"):
                        text = json.dumps(event)
                        if not started and not output_accepted and contains_exhaustion(text):
                            self.state.store.mark_exhausted(account.id, text)
                            return "retry"
                        raise ProtocolError("upstream error event")
                    if (kind in ("response.reasoning_text.delta", "response.reasoning_summary_text.delta", "response.output_text.delta")
                            or (kind == "response.output_item.added" and (event.get("item") or {}).get("type") == "function_call")):
                        output_accepted = True
                    packets = translator.feed(event)
                    if kind == "response.completed":
                        ended = True
                    if started:
                        for packet in packets:
                            self._emit(packet)
                    else:
                        buffered.extend(packets)
                        if packets:
                            started = True
                            self._start_stream([])
                            for packet in buffered:
                                self._emit(packet)
                            buffered = []
                    if ended:
                        break
                if not ended:
                    raise ProtocolError("stream ended without completion")
                finish_reason, usage = translator.finish()
                self._deliver(translator, finish_reason, usage)
                return "done"
            except ProtocolError as error:
                log.warning("request %s: protocol error: %s", self.request_id, error)
                if started or output_accepted:
                    if not self._stream_started:
                        _error(self.handler, 502, "upstream protocol error", "upstream_error")
                    return "protocol"  # never retry or send a successful finish
                return self._pre_output_failure(account, attempt)
            except (OSError, http.client.HTTPException):
                log.warning("request %s: upstream stream aborted", self.request_id)
                if started or output_accepted:
                    if not self._stream_started:
                        _error(self.handler, 502, "upstream stream aborted", "upstream_error")
                    return "fatal"
                self.state.store.cooldown(account.id)
                return "retry"
        finally:
            response.close()

    def _read_body(self, error):
        try:
            return (error.read() or b"").decode("utf-8", "replace")
        except Exception:
            return ""

    def _handle_failure(self, account, status, body):
        if contains_exhaustion(body):
            self.state.store.mark_exhausted(account.id, body)
            return "retry"
        if status == 429 or status >= 500:
            self.state.store.cooldown(account.id)
            return "retry"
        # 400/401/403/404 etc: a request or credential problem, never quota.
        _error(self.handler, 502, "upstream rejected request", "upstream_error")
        return "fatal"

    def _pre_output_failure(self, account, attempt):
        self.state.store.cooldown(account.id)
        if attempt < self.state.max_attempts:
            return "retry"
        _error(self.handler, 502, "upstream protocol error", "upstream_error")
        return "fatal"

    # -- client output ----------------------------------------------------

    def _start_stream(self, packets):
        if not self.stream or self._stream_started:
            return
        self._stream_started = True
        self.handler.send_response(200)
        self.handler.send_header("Content-Type", "text/event-stream")
        self.handler.send_header("Cache-Control", "no-cache")
        self.handler.send_header("Connection", "close")
        for key, value in CORS_HEADERS.items():
            self.handler.send_header(key, value)
        self.handler.end_headers()
        self.handler.close_connection = True
        for packet in packets:
            self._write_chunk(packet["delta"])

    def _emit(self, packet):
        if self.stream:
            self._write_chunk(packet["delta"])

    def _write_chunk(self, delta, finish_reason=None, usage=None):
        chunk = _chunk([{"index": 0, "delta": delta, "finish_reason": finish_reason}],
                       model=self.model)
        if usage:
            chunk["usage"] = usage
        self.handler.wfile.write(b"data: " + json.dumps(chunk).encode() + b"\n\n")
        self.handler.wfile.flush()

    def _deliver(self, translator, finish_reason, usage):
        if self.stream:
            self._start_stream([])
            self._write_chunk({}, finish_reason,
                              openai_usage(usage) if usage else None)
            self.handler.wfile.write(b"data: [DONE]\n\n")
            self.handler.wfile.flush()
            return
        message = {"role": "assistant", "content": translator.content or None}
        if translator.reasoning:
            message["reasoning_content"] = translator.reasoning
        if translator.calls:
            message["tool_calls"] = [
                {"id": c["id"], "type": "function",
                 "function": {"name": c["name"], "arguments": c["arguments"]}}
                for c in translator.calls
            ]
        payload = {
            "id": "chatcmpl-gateway", "object": "chat.completion",
            "created": int(time.time()), "model": self.model,
            "choices": [{"index": 0, "message": message, "finish_reason": finish_reason}],
        }
        if usage:
            payload["usage"] = openai_usage(usage)
        _json_response(self.handler, 200, payload)


class GatewayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_server(host, port, store, upstream_url, api_key, timeout=300,
                max_attempts=8, max_body_bytes=16 * 1024 * 1024):
    if not api_key:
        raise ValueError("api_key is required")
    handler = type("BoundGatewayHandler", (GatewayRequestHandler,),
                   {"state": GatewayState(store, upstream_url, api_key, timeout,
                                          max_attempts, max_body_bytes)})
    return GatewayServer((host, port), handler)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

DB_URL = "https://cli-chat-proxy.grok.com/v1/responses"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Grok gateway")
    parser.add_argument("--host", default=os.environ.get("GATEWAY_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("GATEWAY_PORT", "8787")))
    parser.add_argument("--db", default=os.environ.get("GROK_DB_PATH"))
    parser.add_argument("--state", default=os.environ.get("GATEWAY_STATE", "state.json"))
    parser.add_argument("--upstream", default=os.environ.get("GROK_UPSTREAM_URL", DB_URL))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    api_key = os.environ.get("GATEWAY_API_KEY")
    if not api_key:
        parser.error("GATEWAY_API_KEY is required")
    if not args.db:
        parser.error("--db or GROK_DB_PATH is required")
    if not args.upstream.startswith("https://"):
        parser.error("upstream must be https")
    store = AccountStore(load_accounts(args.db), args.state, source=args.db)
    store.check_permissions()
    server = make_server(
        args.host, args.port, store, args.upstream, api_key,
        timeout=float(os.environ.get("GROK_TIMEOUT", "300")),
        max_attempts=int(os.environ.get("GROK_MAX_ATTEMPTS", "8")),
    )
    log.info("gateway listening on %s:%d", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
