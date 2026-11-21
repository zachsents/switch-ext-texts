"""sbmcp: an MCP server for a Switchboard appendage, in one file, with
nothing to install (Python 3.9+, the standard library only).

An appendage is an MCP server its keeper runs and hosts: its tools are its
actions, and what it emits lands in the brain. This gives a script both
halves:

    import sbmcp

    app = sbmcp.Server("kiln-watch", instructions="Watches the big kiln.")
    app.event("firing", "A firing started, changed or finished",
              stream="kiln/firings", kind="observation")

    @app.tool(read_only=True)
    def status(kiln: str = "big"):
        "How the kiln stands now."
        return {"kiln": kiln, "temperature": read_kiln(kiln)}

    @app.background
    def watch():
        while True:
            app.emit("firing", {"entity": "kiln:big", "summary": "Firing at 1200°F",
                                "facts": {"temperature": 1200}})
            time.sleep(60)

    app.run()

A tool's schema comes from its signature (or give `schema=`), its
description from its docstring. It returns a string, a dict (structured
content), or `sbmcp.content(...)` blocks (`sbmcp.text`, `sbmcp.image`);
raising makes the call fail with the error's words. It gets a `Context` as
`ctx` when it takes one: `ctx.progress("Halfway")`, `ctx.cancelled`,
`ctx.work` (the work that asked).

Events follow MCP's Events extension (SEP-3415), pushed on `events/stream`
or polled with `events/poll`. `event()` says where an event type's
occurrences land in the brain (`stream`, `kind`, `retention`); `emit()`
sends one, with the rest of its record's envelope if it has any (`about`,
`refs`, `importance`, a fixed `id`, a `stream` within the type's pattern).
What's emitted before the keeper has subscribed waits for it.

What its own `sb` reaches beyond where its events land, it declares, and
its keeper grants it when it starts, with no reinstall: `requests` names the
appendages it asks with `sb request` (each both ways, since the answer comes
back where the request went), `notifies=True` says it runs `sb notify`, and
`reads` and `writes` name any other streams (`sb query` of another
appendage's records, `sb subscribe`):

    app = sbmcp.Server("ccc-order", instructions="…",
                       requests=["macos@local", "gmail@switchboard"], notifies=True)

The server speaks both eras of MCP: the stateless `2026-07-28` revision
(`server/discover`, `_meta` on each request) and the `initialize`
handshake of `2025-11-25` and before.

Its stdout is MCP's alone: from the moment it's imported, `print()` and
what child processes print go to stderr, which is the appendage's log.

`Client` speaks MCP to a server the way its keeper does, for its tests:
the command as it will run (`Client(["python3", "main.py"])`), or the
`Server` in the test's own process (`Client(main.app)`), after putting
fakes in place of whatever it reaches outside:

    main.fetch = lambda kiln: {"temperature": 1200}
    with sbmcp.Client(main.app) as mcp:
        assert mcp.tools()["status"]["annotations"]["readOnlyHint"]
        assert mcp.call("status", kiln="big") == {"kiln": "big", "temperature": 1200}
        main.watch_once()
        assert mcp.emitted("firing")[-1]["facts"]["temperature"] == 1200

`sb new <name>` writes a new appendage's folder with all of this in place,
and `sb probe -- python3 main.py` says what its keeper will find in it.
"""

import atexit
import inspect
import itertools
import json
import os
import queue
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone

__all__ = ["Server", "Context", "Client", "CallFailed", "McpError", "content", "text", "image", "audio"]

# MCP gets the real stdout; everything else that writes to fd 1 (print, a
# subprocess that inherits it) goes to stderr, so nothing corrupts the stream.
_MCP = os.fdopen(os.dup(1), "w", buffering=1, encoding="utf-8")
os.dup2(2, 1)
sys.stdout = os.fdopen(1, "w", buffering=1, encoding="utf-8", closefd=False)
atexit.register(_MCP.close)

MODERN = "2026-07-28"
LEGACY = ["2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]
EVENTS = "io.modelcontextprotocol/events"
RECORD = "switchboard/record"
CONNECTIONS = "ax.switch/connections"
REACHES = "ax.switch/reaches"
SCOPES = "switchboard/scopes"
SUBSCRIPTION = "io.modelcontextprotocol/subscriptionId"
BUFFERED = 1000
# Cursors are good only within this run: its buffer of what it emitted goes with it.
EPOCH = uuid.uuid4().hex[:8]


def text(words):
    """A text content block."""
    return {"type": "text", "text": str(words)}


def image(data, mime="image/png"):
    """An image content block, from bytes or a path."""
    return _media("image", data, mime)


def audio(data, mime="audio/mpeg"):
    """An audio content block, from bytes or a path."""
    return _media("audio", data, mime)


def _media(kind, data, mime):
    import base64
    if isinstance(data, (str, os.PathLike)):
        with open(data, "rb") as f:
            data = f.read()
    return {"type": kind, "data": base64.b64encode(data).decode(), "mimeType": mime}


class content(list):
    """A tool's result as content blocks: `return sbmcp.content(text("…"), image(png))`."""

    def __init__(self, *blocks, structured=None):
        super().__init__(blocks)
        self.structured = structured


class Context:
    """What a tool call knows of itself."""

    def __init__(self, server, request_id, meta):
        self._server = server
        self.request_id = request_id
        self.meta = meta or {}
        asked = self.meta.get("switchboard/request") or {}
        self.work = asked.get("work")
        """The work that asked, when it's a turn: keep things per piece of work by it."""
        self.asker = asked.get("from")
        self.cancelled = threading.Event()
        """Set when the asker gave up: stop when it is."""
        self._done = 0

    def progress(self, message, progress=None, total=None):
        """Tells the asker how it's going."""
        token = self.meta.get("progressToken")
        if token is None:
            return
        self._done = progress if progress is not None else self._done + 1
        params = {"progressToken": token, "progress": self._done, "message": str(message)}
        if total is not None:
            params["total"] = total
        self._server._notify("notifications/progress", params)


class Server:
    def __init__(self, name, version="1", instructions=None, title=None, requests=None, notifies=False, reads=None, writes=None,
                 connections=None, reaches=None):
        """`connections` (in Switch): the accounts it needs, each {provider, scopes, multiple, kind?, label?}; it reaches
        them with accounts(), token() and adopt(), through whoever runs it. `reaches`: {calls, follows}, the other
        extensions' tools it calls with call() and the brain's event types it follows with follow(), names or prefixes
        ending in *.
        `requests`: the appendages it asks with `sb request`, like "macos@local" or "macos@*";
        `notifies`: it runs `sb notify`; `reads` and `writes`: other streams its `sb` reads or writes.
        Its keeper grants them when it starts."""
        self.name, self.version, self.instructions, self.title = name, str(version), instructions, title
        self.scopes = {k: v for k, v in (("requests", _strings(requests)), ("notifies", bool(notifies)),
                                         ("read", _strings(reads)), ("write", _strings(writes))) if v}
        """What it declared its own `sb` reaches, as its handshake says it (`switchboard/scopes`)."""
        self._tools = {}
        self._events = {}
        self._out = threading.Lock()
        self._subs = {}  # subscription id -> (event name, heartbeat seconds)
        self._subs_lock = threading.Lock()
        self._waiting = {}  # event name -> occurrences emitted before anyone subscribed
        self._log = {}  # event name -> [(seq, occurrence)], for polls
        self._seq = 0
        self._calls = {}  # request id -> Context
        self._background = []
        self._output = _MCP
        self.connections = list(connections or [])
        self.reaches = dict(reaches or {})
        self._on_event = []
        self._asked = {}  # request id -> [Event, answer]
        self._ready = threading.Event()  # set once the client finished its handshake: then it can be asked
        self._asks = 0

    # Declaring

    def tool(self, fn=None, *, name=None, description=None, schema=None, output_schema=None, title=None,
             read_only=False, speaks_for_person=False, destructive=None, idempotent=None, open_world=None):
        """Declares a tool. `read_only=True` marks one that only looks, which chat work may call;
        `speaks_for_person=True` one that sends something out in the person's name (a text, an
        email, a call), which Switchboard's outbound gates read before it runs."""

        def declare(fn):
            annotations = {"readOnlyHint": bool(read_only)}
            if speaks_for_person:
                annotations["speaksForPerson"] = True
            for key, value in (("destructiveHint", destructive), ("idempotentHint", idempotent), ("openWorldHint", open_world)):
                if value is not None:
                    annotations[key] = bool(value)
            tool = {
                "name": name or fn.__name__,
                "description": (description or inspect.getdoc(fn) or "").strip(),
                "inputSchema": schema or _schema(fn),
                "annotations": annotations,
            }
            if title:
                tool["title"] = title
            if output_schema:
                tool["outputSchema"] = output_schema
            self._tools[tool["name"]] = (tool, fn)
            return fn

        return declare(fn) if fn else declare

    def event(self, name, description="", stream=None, kind=None, retention=None, payload=None, arguments=None):
        """Declares an event type, and where its occurrences land in the brain."""
        placement = {k: v for k, v in (("stream", stream), ("kind", kind), ("retention", retention)) if v}
        declared = {
            "name": name, "description": description, "delivery": ["push", "poll"],
            "inputSchema": arguments or {"type": "object", "properties": {}},
            "payloadSchema": payload or {"type": "object"},
        }
        if placement:
            declared["_meta"] = {RECORD: placement}
        self._events[name] = declared
        self._waiting.setdefault(name, [])
        self._log.setdefault(name, [])

    def background(self, fn):
        """Runs `fn` in a thread of its own once the server starts: a sense's loop."""
        self._background.append(fn)
        return fn

    # Emitting

    def emit(self, name, data, about=None, refs=None, importance=None, id=None, stream=None, event_id=None, at=None,
             sender=None, message=None):
        """Sends one occurrence of an event type to whoever subscribed (the keeper), which
        records it where the type lands. `refs` are "rel:id" strings, like "supersedes:01ab…". In Switch, a message
        says who sent it (`sender`: handle, via, and name, me or agent) and what it is (`message`: id,
        conversation, text, at, and subject, unread or channel)."""
        if name not in self._events:
            raise ValueError(f"declare the event {name!r} with event() first")
        envelope = {k: v for k, v in (("about", about), ("refs", refs), ("importance", importance), ("id", id), ("stream", stream),
                                                    ("sender", sender), ("message", message)) if v is not None}
        occurrence = {
            "eventId": event_id or uuid.uuid4().hex,
            "name": name,
            "timestamp": _stamp(at),
            "data": data,
            "cursor": None,
        }
        if envelope:
            occurrence["_meta"] = {RECORD: envelope}
        with self._subs_lock:
            self._seq += 1
            log = self._log[name]
            log.append((self._seq, occurrence))
            del log[:-BUFFERED]
            occurrence["cursor"] = f"{EPOCH}:{self._seq}"
            subs = [sid for sid, (event, _) in self._subs.items() if event == name]
            if not subs:
                waiting = self._waiting[name]
                waiting.append(occurrence)
                del waiting[:-BUFFERED]
                return
        for sid in subs:
            self._send_event(sid, occurrence)

    def _send_event(self, sid, occurrence):
        params = dict(occurrence)
        params["_meta"] = {**occurrence.get("_meta", {}), SUBSCRIPTION: sid}
        self._notify("notifications/events/event", params)

    def log(self, *words):
        """A line for the appendage's log."""
        print(*words, file=sys.stderr, flush=True)

    # Serving

    def run(self):
        """Serves MCP on stdin and stdout until stdin closes."""
        self.serve(sys.stdin, _MCP)
        # stdin closed: the keeper is done with this server.
        _MCP.flush()

    def serve(self, lines, output, background=True):
        """Serves MCP on any stream of lines until it ends, answering on `output` (anything with
        `write`): `run()` on stdin and stdout, or a `Client` in the same process. `background`
        starts its background loops too. One stream at a time."""
        self._output = output
        if background:
            for fn in self._background:
                threading.Thread(target=self._guarded, args=(fn,), daemon=True).start()
        over = threading.Event()
        threading.Thread(target=self._heartbeats, args=(over,), daemon=True).start()
        try:
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                try:
                    message = json.loads(line)
                except ValueError:
                    continue
                if isinstance(message, dict):
                    self._received(message)
        finally:
            over.set()

    def _guarded_event(self, fn, event):
        try:
            fn(event)
        except Exception:
            self.log(f"{fn.__name__}: {traceback.format_exc()}")

    def _guarded(self, fn):
        try:
            fn()
        except Exception:
            self.log(f"{fn.__name__} stopped:\n{traceback.format_exc()}")
            # A sense that dies takes the server with it, so its keeper starts it again.
            os._exit(1)

    def _received(self, message):
        method, mid, params = message.get("method"), message.get("id"), message.get("params") or {}
        if method is None:
            waiting = self._asked.pop(mid, None) if isinstance(mid, str) else None
            if waiting:
                waiting[1] = message
                waiting[0].set()
            return
        if mid is None:
            if method == "notifications/initialized":
                self._ready.set()
                return
            if method == "switch/event":
                for types, fn in self._on_event:
                    if any(params.get("type", "").startswith(t[:-1]) if t.endswith("*") else params.get("type") == t for t in types):
                        self._guarded_event(fn, params)  # in order, on this thread: fn should be quick
                return
            if method == "notifications/cancelled":
                self._cancelled(params.get("requestId"))
            return
        try:
            if method == "tools/call":
                threading.Thread(target=self._call, args=(mid, params), daemon=True).start()
                return
            if method == "events/stream":
                self._stream(mid, params)
                return
            if method == "subscriptions/listen":
                return  # nothing here changes; the stream just stays open
            result = self._answer(method, params)
        except _Error as e:
            self._reply_error(mid, e.code, e.message, e.data)
            return
        except Exception as e:
            self._reply_error(mid, -32603, f"{type(e).__name__}: {e}")
            return
        self._reply(mid, result)

    def _answer(self, method, params):
        declared = {SCOPES: self.scopes} if self.scopes else {}
        if method == "server/discover":
            return {"supportedVersions": [MODERN] + LEGACY, "capabilities": self._capabilities(),
                    "instructions": self.instructions or "", "ttlMs": 0,
                    "_meta": {"io.modelcontextprotocol/serverInfo": self._info(), **declared}}
        if method == "initialize":
            asked = params.get("protocolVersion")
            return {"protocolVersion": asked if asked in LEGACY else LEGACY[0], "capabilities": self._capabilities(),
                    "serverInfo": self._info(), **({"instructions": self.instructions} if self.instructions else {}),
                    **({"_meta": declared} if declared else {})}
        if method == "ping":
            return {}
        if method == "tools/list":
            self._ready.set()
            return {"tools": [tool for tool, _ in self._tools.values()]}
        if method == "events/list":
            return {"events": list(self._events.values())}
        if method == "events/poll":
            return self._poll(params)
        raise _Error(-32601, f"Method not found: {method}")

    def _capabilities(self):
        extensions = {EVENTS: {"listChanged": False}}
        if self.connections:
            extensions[CONNECTIONS] = {"needs": self.connections}
        if self.reaches:
            extensions[REACHES] = self.reaches
        return {"tools": {"listChanged": False}, "extensions": extensions}

    # Asking whoever runs it (in Switch: the user's connections)

    def ask(self, method, params, timeout=60):
        """A request to the client, answered on stdin: its result, or RuntimeError with what it said. Waits for the
        client's handshake first."""
        if not self._ready.wait(timeout):
            raise RuntimeError(f"{method}: no client yet")
        with self._out:
            self._asks += 1
            mid = f"ask-{self._asks}"
            waiting = [threading.Event(), None]
            self._asked[mid] = waiting
        self._write({"jsonrpc": "2.0", "id": mid, "method": method, "params": params})
        if not waiting[0].wait(timeout):
            self._asked.pop(mid, None)
            raise RuntimeError(f"{method}: no answer")
        answer = waiting[1] or {}
        if "error" in answer:
            raise RuntimeError(f"{method}: {(answer['error'] or {}).get('message', 'refused')}")
        return answer.get("result") or {}

    def call(self, name, arguments=None, timeout=120):
        """Another extension's tool (one it declared in reaches.calls), as the agent would call it: its result."""
        return self.ask("switch/call", {"name": name, "arguments": arguments or {}}, timeout=timeout)

    def follow(self, types, fn):
        """Calls fn(event) for each of the brain's events of these types (declared in reaches.follows) from now on:
        event is {id, type, source, time, payload}."""
        self._on_event.append((list(types), fn))
        return self.ask("switch/follow", {"types": list(types)})

    def accounts(self, provider):
        """The user's connected accounts for a provider it declared: [{account, scopes}]."""
        return self.ask("connections/list", {"provider": provider}).get("accounts") or []

    def token(self, provider, account):
        """A fresh access token (or a key's secret) and when it expires (epoch ms): (token, expires)."""
        t = self.ask("connections/token", {"provider": provider, "account": account})
        return t["token"], t.get("expires")

    def adopt(self, provider, account, grant):
        """Hands a grant it kept itself before connections (refresh_token, scope, client, client_kind; or secret) to
        the user's connections, once: after this, it keeps no copy."""
        return self.ask("connections/adopt", {"provider": provider, "account": account, "grant": grant}).get("id")

    def _info(self):
        info = {"name": self.name, "version": self.version}
        if self.title:
            info["title"] = self.title
        return info

    def _call(self, mid, params):
        name, args = params.get("name"), params.get("arguments") or {}
        found = self._tools.get(name)
        if not found:
            self._reply_error(mid, -32602, f"Unknown tool: {name}")
            return
        _, fn = found
        ctx = Context(self, mid, params.get("_meta"))
        self._calls[mid] = ctx
        try:
            takes = inspect.signature(fn).parameters
            kwargs = {k: v for k, v in args.items() if k in takes or any(p.kind == p.VAR_KEYWORD for p in takes.values())}
            if "ctx" in takes:
                kwargs["ctx"] = ctx
            result = _result(fn(**kwargs))
        except Exception as e:
            result = {"content": [text(f"{type(e).__name__}: {e}" if not str(e) else str(e))], "isError": True}
        finally:
            self._calls.pop(mid, None)
        if not ctx.cancelled.is_set():
            self._reply(mid, result)

    def _cancelled(self, request_id):
        ctx = self._calls.get(request_id)
        if ctx:
            ctx.cancelled.set()
        with self._subs_lock:
            self._subs.pop(request_id, None)

    def _stream(self, mid, params):
        name = params.get("name")
        if name not in self._events:
            raise _Error(-32023, "NotFound", {"kind": "event"})
        heartbeat = max(5.0, min(float(params.get("heartbeatMs") or 30000) / 1000, 300.0))
        with self._subs_lock:
            self._subs[mid] = (name, heartbeat)
            waiting, self._waiting[name] = self._waiting[name], []
            cursor = f"{EPOCH}:{self._seq}"
        self._notify("notifications/events/active", {"cursor": cursor, "truncated": False, "heartbeatMs": int(heartbeat * 1000), "_meta": {SUBSCRIPTION: mid}})
        for occurrence in waiting:
            self._send_event(mid, occurrence)

    def _poll(self, params):
        name = params.get("name")
        if name not in self._events:
            raise _Error(-32023, "NotFound", {"kind": "event"})
        most = int(params.get("maxEvents") or 100)
        with self._subs_lock:
            cursor = params.get("cursor")
            if cursor is None:
                return {"events": [], "cursor": f"{EPOCH}:{self._seq}", "truncated": False, "hasMore": False, "nextPollMs": 5000}
            epoch, _, seq = str(cursor).partition(":")
            # A cursor from an earlier run: everything this one still has, and maybe a gap.
            after = int(seq) if epoch == EPOCH and seq.isdigit() else 0
            log = self._log[name]
            newer = [o for seq, o in log if seq > after]
            truncated = epoch != EPOCH or (bool(log) and log[0][0] > after + 1)
            batch = newer[:most]
            last = batch[-1]["cursor"] if batch else f"{EPOCH}:{max(after, self._seq)}"
        return {"events": batch, "cursor": last, "truncated": truncated, "hasMore": len(newer) > most, "nextPollMs": 5000}

    def _heartbeats(self, over):
        beat = {}
        while not over.wait(1):
            now = time.monotonic()
            with self._subs_lock:
                subs = list(self._subs.items())
                cursor = f"{EPOCH}:{self._seq}"
            for sid, (_, every) in subs:
                if now - beat.setdefault(sid, now) >= every:
                    beat[sid] = now
                    self._notify("notifications/events/heartbeat", {"cursor": cursor, "_meta": {SUBSCRIPTION: sid}})

    def _write(self, message):
        line = json.dumps(message, separators=(",", ":"), default=str)
        with self._out:
            self._output.write(line + "\n")
            self._output.flush()

    def _reply(self, mid, result):
        if isinstance(result, dict):
            result.setdefault("resultType", "complete")
        self._write({"jsonrpc": "2.0", "id": mid, "result": result})

    def _reply_error(self, mid, code, message, data=None):
        error = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        self._write({"jsonrpc": "2.0", "id": mid, "error": error})

    def _notify(self, method, params):
        self._write({"jsonrpc": "2.0", "method": method, "params": params})


class _Error(Exception):
    def __init__(self, code, message, data=None):
        super().__init__(message)
        self.code, self.message, self.data = code, message, data


class McpError(Exception):
    """An error a server answered a request with."""

    def __init__(self, code, message, data=None):
        super().__init__(f"{message} ({code})" + (f": {json.dumps(data)}" if data is not None else ""))
        self.code, self.message, self.data = code, message, data


class CallFailed(Exception):
    """A tool call that failed, with the words it failed with: what the asker of the action sees."""


_ids = itertools.count(1)


class Client:
    """Speaks MCP to a server the way its keeper does, for tests and trying it by hand.

    `server` is the command that runs it (a list, or a shell line), run on stdio as its keeper
    runs it, or a `Server` in this process, which lets a test put fakes in place of what it reaches
    outside first (its background loops don't run unless `background=True`). `env` adds to this
    process's environment, `cwd` is where the command runs, and `timeout` is how many seconds any
    request waits. It discovers the server as it starts (`server/discover`, else the `initialize`
    handshake): `server`, `instructions`, `protocol` and `scopes` (what it declared its `sb`
    reaches) say what it found. Close it, or use it in a `with`."""

    def __init__(self, server, env=None, cwd=None, background=False, timeout=60):
        self.timeout = timeout
        self.notes = []
        """Every notification it sent that isn't an event's, as (method, params): progress, logging."""
        self.occurrences = {}
        """What it emitted on each subscription, by event name: each occurrence as sent."""
        self._answers, self._streams, self._log = {}, {}, []
        self._cv = threading.Condition()
        self._modern = True
        self._process = self._inbox = None
        if isinstance(server, Server):
            self._inbox = queue.Queue()
            served = threading.Thread(target=server.serve, args=(iter(self._inbox.get, None), _Lines(self._received)), kwargs={"background": background}, daemon=True)
            served.start()
        else:
            self._process = subprocess.Popen(server, shell=isinstance(server, str), cwd=cwd, env={**os.environ, **(env or {})},
                                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                             text=True, encoding="utf-8", errors="replace", bufsize=1)
            self._stdin = threading.Lock()
            self._readers = [threading.Thread(target=self._read, daemon=True), threading.Thread(target=self._read_log, daemon=True)]
            for reader in self._readers:
                reader.start()
        try:
            self._discover()
        except Exception:
            self.close()
            raise

    # What it offers

    def tools(self):
        """Its tools, by name, as `tools/list` gives them."""
        return {t["name"]: t for t in self._list("tools/list", "tools")}

    def events(self):
        """Its event types, by name, as `events/list` gives them (with where each lands)."""
        return {e["name"]: e for e in self._list("events/list", "events")}

    # Using it

    def call(self, tool, /, **arguments):
        """Calls a tool and returns what its asker gets, as its keeper turns it into an action's
        output: its structured content, else its text (parsed when it's JSON). Raises `CallFailed`
        with its words when it fails."""
        result = self.result(tool, **arguments)
        words = "\n".join(b.get("text", "") for b in result.get("content", []) if b.get("type") == "text")
        if result.get("isError"):
            raise CallFailed(words or "it failed")
        if result.get("structuredContent") is not None:
            return result["structuredContent"]
        try:
            parsed = json.loads(words)
        except ValueError:
            return words
        return parsed if isinstance(parsed, (dict, list)) else words

    def result(self, tool, /, **arguments):
        """Calls a tool and returns its MCP result as it is (content blocks, isError), as the work
        `work/test` asking. An unknown tool raises `CallFailed`."""
        try:
            return self.request("tools/call", {"name": tool, "arguments": arguments,
                                               "_meta": {"progressToken": f"call-{next(_ids)}", "switchboard/request": {"work": "work/test"}}})
        except McpError as e:
            raise CallFailed(e.message) from e

    def subscribe(self, event, arguments=None):
        """Subscribes to an event type as its keeper does (`events/stream`): what it emits from
        then on (and what waited for a subscriber) collects in `occurrences[event]`."""
        mid = next(_ids)
        with self._cv:
            self._streams[mid] = event
            self.occurrences.setdefault(event, [])
        self._send({"jsonrpc": "2.0", "id": mid, "method": "events/stream",
                    "params": self._meta({"name": event, "arguments": arguments or {}, "cursor": None, "heartbeatMs": 30000})})

    def emitted(self, event, count=1, within=None):
        """The data of what it emitted of an event type since subscribing (subscribing first if it
        hasn't), once there are at least `count`: within `within` seconds (default `timeout`), or
        it raises TimeoutError."""
        if event not in self._streams.values():
            self.subscribe(event)
        end = time.monotonic() + (self.timeout if within is None else within)
        with self._cv:
            while len(self.occurrences[event]) < count:
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"{event}: {len(self.occurrences[event])} emitted, not {count}" + self._said())
                self._cv.wait(left)
            return [o.get("data") for o in self.occurrences[event]]

    def ping(self):
        """Pings it, as its keeper does every minute: one that stops answering is restarted."""
        return self.request("ping", timeout=20)

    def request(self, method, params=None, timeout=None):
        """One request, answered within `timeout` seconds (default the client's), or TimeoutError.
        An error it answers with raises `McpError`."""
        mid = next(_ids)
        self._send({"jsonrpc": "2.0", "id": mid, "method": method, "params": self._meta(params or {})})
        end = time.monotonic() + (self.timeout if timeout is None else timeout)
        with self._cv:
            while mid not in self._answers:
                if self._process and self._process.poll() is not None:
                    raise McpError(-32099, f"the server stopped ({self._process.returncode}) before answering {method}" + self._said())
                left = end - time.monotonic()
                if left <= 0:
                    raise TimeoutError(f"no answer to {method} within {self.timeout if timeout is None else timeout}s" + self._said())
                self._cv.wait(min(left, 0.5))
            answer = self._answers.pop(mid)
        if "error" in answer:
            error = answer["error"] or {}
            raise McpError(error.get("code", -32603), error.get("message", "it failed"), error.get("data"))
        return answer.get("result") or {}

    @property
    def log(self):
        """What the command has written to stderr, its log (nothing for a server in this process)."""
        with self._cv:
            return "".join(self._log)

    def close(self):
        """Ends its subscriptions and stops it: a command's stdin is closed, as its keeper's is when
        it's done, and it's killed if it hasn't exited 10 seconds later."""
        for mid in list(self._streams):
            try:
                self._send({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": mid, "reason": "the test is done"}})
            except (OSError, ValueError):
                break
        self._streams.clear()
        if self._inbox is not None:
            self._inbox.put(None)
        if self._process is not None:
            try:
                self._process.stdin.close()
            except OSError:
                pass
            try:
                self._process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
            for reader in self._readers:
                reader.join(timeout=2)
            self._process.stdout.close()
            self._process.stderr.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    # Speaking

    def _discover(self):
        try:
            found = self.request("server/discover", timeout=self.timeout)
            self._modern = MODERN in (found.get("supportedVersions") or [])
        except McpError:
            self._modern, found = False, None
        if self._modern:
            self.protocol, self.capabilities = MODERN, found.get("capabilities") or {}
            self.server = (found.get("_meta") or {}).get("io.modelcontextprotocol/serverInfo") or {}
            self.instructions = found.get("instructions") or ""
            self.scopes = (found.get("_meta") or {}).get(SCOPES) or {}
            return
        found = self.request("initialize", {"protocolVersion": LEGACY[0], "capabilities": {"extensions": {EVENTS: {}}},
                                            "clientInfo": {"name": "sbmcp-client", "version": "1"}})
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
        self.protocol, self.capabilities = found.get("protocolVersion"), found.get("capabilities") or {}
        self.server, self.instructions = found.get("serverInfo") or {}, found.get("instructions") or ""
        self.scopes = (found.get("_meta") or {}).get(SCOPES) or {}

    def _meta(self, params):
        if not self._modern:
            return params
        meta = {"io.modelcontextprotocol/protocolVersion": MODERN, "io.modelcontextprotocol/clientCapabilities": {"extensions": {EVENTS: {}}},
                "io.modelcontextprotocol/clientInfo": {"name": "sbmcp-client", "version": "1"}}
        return {**params, "_meta": {**meta, **(params.get("_meta") or {})}}

    def _list(self, method, field):
        found, cursor = [], None
        while True:
            page = self.request(method, {"cursor": cursor} if cursor else {})
            found += page.get(field) or []
            cursor = page.get("nextCursor")
            if not cursor:
                return found

    def _send(self, message):
        line = json.dumps(message, separators=(",", ":"), default=str)
        if self._inbox is not None:
            self._inbox.put(line)
            return
        with self._stdin:
            self._process.stdin.write(line + "\n")
            self._process.stdin.flush()

    def _received(self, message):
        with self._cv:
            if "id" in message and ("result" in message or "error" in message):
                self._answers[message["id"]] = message
            elif message.get("method") == "notifications/events/event":
                params = message.get("params") or {}
                event = self._streams.get((params.get("_meta") or {}).get(SUBSCRIPTION))
                if event is not None:
                    self.occurrences[event].append(params)
            elif "method" in message and "id" not in message:
                self.notes.append((message["method"], message.get("params") or {}))
            self._cv.notify_all()

    def _read(self):
        for line in self._process.stdout:
            try:
                message = json.loads(line)
            except ValueError:
                with self._cv:
                    self._log.append(f"(on stdout, which is MCP's) {line}")
                continue
            if isinstance(message, dict):
                self._received(message)
        with self._cv:
            self._cv.notify_all()

    def _read_log(self):
        for line in self._process.stderr:
            with self._cv:
                self._log.append(line)

    def _said(self):
        log = "".join(self._log[-20:]).rstrip()
        return f"; it said:\n{log}" if log else ""


class _Lines:
    """What a server in the same process writes, read as it goes, a line at a time."""

    def __init__(self, received):
        self._received, self._rest = received, ""

    def write(self, text):
        self._rest += text
        *lines, self._rest = self._rest.split("\n")
        for line in lines:
            if line.strip():
                self._received(json.loads(line))

    def flush(self):
        pass


def _result(value):
    """What a tool returned, as MCP's result."""
    if isinstance(value, content):
        result = {"content": list(value)}
        if value.structured is not None:
            result["structuredContent"] = value.structured
        return result
    if isinstance(value, dict) and value.get("type") in ("text", "image", "audio", "resource", "resource_link"):
        return {"content": [value]}
    if isinstance(value, (dict, list)):
        result = {"content": [text(json.dumps(value, ensure_ascii=False, default=str))]}
        if isinstance(value, dict):
            result["structuredContent"] = value
        return result
    return {"content": [text("" if value is None else value)]}


def _strings(values):
    """A list of strings from one string or several, in order and each once."""
    if isinstance(values, str):
        values = [values]
    seen = []
    for value in values or []:
        if str(value).strip() and str(value).strip() not in seen:
            seen.append(str(value).strip())
    return seen


def _stamp(at):
    when = datetime.fromtimestamp(at / 1000, timezone.utc) if isinstance(at, (int, float)) else (at or datetime.now(timezone.utc))
    return when.isoformat(timespec="milliseconds").replace("+00:00", "Z")


_TYPES = {str: "string", int: "integer", float: "number", bool: "boolean", list: "array", dict: "object"}


def _schema(fn):
    """A tool's input schema from its signature: each parameter's type from its annotation or
    default, required when it has no default."""
    properties, required = {}, []
    for name, p in inspect.signature(fn).parameters.items():
        if name == "ctx" or p.kind in (p.VAR_POSITIONAL, p.VAR_KEYWORD):
            continue
        kind = p.annotation if p.annotation is not p.empty else type(p.default) if p.default not in (p.empty, None) else None
        origin = getattr(kind, "__origin__", None) or kind
        properties[name] = {"type": _TYPES[origin]} if origin in _TYPES else {}
        if p.default is p.empty:
            required.append(name)
    schema = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    return schema
