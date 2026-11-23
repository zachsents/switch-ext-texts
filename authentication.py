"""Fresh text verification codes for a bound browser challenge, never message output."""

import re
import threading
import time

_used = set()
_lock = threading.Lock()


def register(app, connect, describe, columns, epoch):
    """Adds a private transport on the existing Messages service and its read-only database."""
    @app.tool(name="authentication_code", read_only=True,
              description="Private authentication transport. Never publish its result or call it from a model.",
              schema={"type": "object", "required": ["sender", "service", "since"], "properties": {
                  "sender": {"type": "string"}, "service": {"type": "string"}, "since": {"type": "number"}}})
    def authentication_code(sender, service, since):
        if not isinstance(since, (int, float)) or not time.time() * 1000 - 600000 <= since <= time.time() * 1000:
            raise ValueError("Challenge expired")
        if not re.fullmatch(r"[+\w@.-]{3,100}", sender) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9 .-]{1,79}", service):
            raise ValueError("A sender and service are required")
        matches = []
        with connect() as con:
            rows = con.execute(f"SELECT {columns} FROM message m LEFT JOIN handle h ON h.ROWID=m.handle_id "
                               "WHERE h.id=? AND m.is_from_me=0 AND m.date>=? ORDER BY m.date DESC LIMIT 5",
                               (sender, int((since / 1000 - epoch) * 1000000000))).fetchall()
            for row in rows:
                d = describe(con, row)
                if not since <= (row["date"] / 1000000000 + epoch) * 1000 <= time.time() * 1000 or service.lower() not in (d["text"] or "").lower():
                    continue
                codes = set(re.findall(r"(?:verification|security|sign[ -]?in|login|one[ -]?time|authentication)\s+code(?:\s+is)?[\s:：-]{0,30}(\d{4,8})\b", d["text"], re.IGNORECASE))
                codes.update(re.findall(r"\b(\d{4,8})\b[^\n]{0,40}(?:verification|security|sign[ -]?in|login|one[ -]?time|authentication)\s+code", d["text"], re.IGNORECASE))
                if len(codes) == 1:
                    matches.append((row["guid"], next(iter(codes))))
        with _lock:
            fresh = [(mid, code) for mid, code in matches if mid not in _used]
            if len(fresh) != 1:
                return {"found": False}
            mid, code = fresh[0]
            _used.add(mid)
        return {"found": True, "code": code}

    app._tools["authentication_code"][0]["_meta"] = {"ui": {"visibility": ["app"]}}
