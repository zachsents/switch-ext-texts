"""texts: sends texts from the Mac mini's Messages (iMessage, RCS or SMS, files included) and says
whether each one landed: sent, delivered, read, or why not, read from Messages' own database.

An appendage: an MCP server its keeper runs and hosts as texts@<machine>. Its tools are its
actions, and what it emits lands in the brain where each event type says (/docs/appendages).

- send: sends text and files to a number, an email, a chat's guid or a group chat's name, then
  waits for Messages to say what became of each part. Files go through a folder Messages may read
  (~/Pictures), since its sandbox silently drops a file sent from anywhere else.
- check: where a send, a message or a whole conversation stands now.
- approval: the drafts, the Send assist and the resolve_when for one approval card.
- status: whether it can send at all (chat.db, Messages, its accounts, Automation).

Each send is the entity text-send:<key> in comms/text-sends, observed again whenever it changes
(delivered, read, failed), so an expectation or an ask can wait on it.

- Its tests: python3 -m unittest -q
- What its keeper will find in it: sb probe -- python3 main.py
- Installing it: sb install texts --dir . --on macos@local --test 'python3 -m unittest -q'
  --try status --health-every 600000 -- python3 main.py
"""

import glob
import hashlib
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import subprocess
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import authentication
import sbmcp

NAME = "texts"
INSTANCE = os.environ.get("SB_INSTANCE") or f"{NAME}@local"
HOME = os.environ.get("SB_HOME") or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".home")
"""Where it keeps what lasts between runs: its keeper's SB_HOME, or .home by hand."""
DB = os.environ.get("TEXTS_DB") or os.path.expanduser("~/Library/Messages/chat.db")
CONTACTS = os.environ.get("TEXTS_CONTACTS") or os.path.expanduser("~/Library/Application Support/AddressBook")
STAGE = os.environ.get("TEXTS_STAGE") or os.path.expanduser("~/Pictures/Switchboard texts")
"""Where files wait to be sent: Messages is sandboxed, and a file sent from outside the folders it
may read (Pictures, Downloads, Documents) leaves an empty attachment bubble and no file."""
HEARTBEAT_S = float(os.environ.get("TEXTS_HEARTBEAT_S") or 600)
TICK_S = float(os.environ.get("TEXTS_TICK_S") or 3)
APPLE_EPOCH = 978307200
WATCH_FOR_S = 48 * 3600
"""How long a send that hasn't landed is watched for its receipt."""
READ_FOR_S = 24 * 3600
"""How long a delivered text is watched for being read."""
SMS_SETTLE_S = 600
"""SMS has no delivery receipt: once sent, it's watched this long for an error, then left as sent."""
FILE_GRACE_S = 60
"""How long a file's message may go without its attachment before the file counts as dropped."""
DUPLICATE_S = 15 * 60
MAX_FILE_BYTES = 100 * 1024 * 1024

app = sbmcp.Server(NAME, instructions=(
    "Texts from the user's Messages on the Mac mini: send (iMessage, RCS or SMS, files included) and see "
    "whether each one was delivered and read. send speaks for the user: only words he asked for or approved "
    "(approval builds the card). check says where any send, message or conversation stands."))
app.event("send", "A text sent through this tool, each time where it stands changes (sent, delivered, read, failed)",
          stream="comms/text-sends", kind="observation")
app.event("message", "A message in Messages, from either side (who, which chat, its words and files), and again once the user reads it",
          stream="comms/imessage", kind="observation")
app.event("heartbeat", "How it is itself", stream=f"vitals/{NAME}", kind="observation", retention="signal")

state = {"started_at": int(time.time() * 1000), "sends": 0, "failed": 0, "failing": {}, "last_failure": None,
         "last_failure_at": None, "last_send": None, "caught_up": False, "caught_up_at": int(time.time() * 1000)}
"""`failing`: what's failing now, by part (watch, messages), each cleared once that part works again;
`last_failure`: the latest failure, kept; `caught_up`: whether the last look read every message chat.db
had, and `caught_up_at` when a look last did."""
SENDS = {}
"""Every send it made lately, by key: what it sent, its parts' message rows, and where it stands."""
SEND_LOCK = threading.Lock()
STATE_LOCK = threading.Lock()


# The world outside: Messages (AppleScript), its database, Contacts, blobs and URLs.

def osascript(script, args, timeout=45):
    """Runs an AppleScript with its arguments: (exit code, stdout, stderr)."""
    try:
        done = subprocess.run(["osascript", "-", *args], input=script, capture_output=True, text=True, timeout=timeout)
        return done.returncode, done.stdout.strip(), done.stderr.strip()
    except subprocess.TimeoutExpired:
        return 124, "", f"osascript didn't finish in {timeout} s (-1712)"


def connect():
    """Messages' database, read-only: a fresh connection sees what Messages wrote since."""
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    return con


def messages_running():
    return subprocess.run(["pgrep", "-x", "Messages"], capture_output=True).returncode == 0


def fetch_blob(blob, dest):
    done = subprocess.run(["sb", "blob", "get", blob, "-o", dest], capture_output=True, text=True, timeout=300)
    if done.returncode != 0:
        raise RuntimeError(f"couldn't fetch {blob}: {done.stderr.strip() or done.stdout.strip()}")


def put_blob(path, mime):
    done = subprocess.run(["sb", "blob", "put", "--type", mime, path], capture_output=True, text=True, timeout=300)
    if done.returncode != 0:
        raise RuntimeError(f"couldn't store {path} as a blob: {done.stderr.strip()}")
    found = re.search(r"sha256:[0-9a-f]{64}", done.stdout)
    if not found:
        raise RuntimeError(f"sb blob put didn't say the blob: {done.stdout.strip()[:200]}")
    return found.group()


def download(url, dest):
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "texts/1"}), timeout=120) as r, open(dest, "wb") as f:
        shutil.copyfileobj(r, f)


# Reading Messages' database

def apple_ms(value):
    """A chat.db time (nanoseconds, or seconds in old rows, since 2001) as epoch milliseconds."""
    if not value:
        return None
    seconds = value / 1e9 if value > 1e11 else value
    return int((seconds + APPLE_EPOCH) * 1000)


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z") if ms else None


def local(ms):
    """A time as the Mac's clock shows it, like 11:00:49 AM PST."""
    if not ms:
        return None
    t = time.localtime(ms / 1000)
    return time.strftime("%-I:%M:%S %p %Z", t) if time.strftime("%Y-%m-%d", t) == time.strftime("%Y-%m-%d") \
        else time.strftime("%a %b %-d %-I:%M %p %Z", t)


def decode_body(blob):
    """The words in an attributedBody (a typedstream NSAttributedString), which is where macOS keeps a
    message's text when its text column is empty, as it is for what AppleScript sends."""
    if not blob:
        return None
    blob = bytes(blob)
    i = blob.find(b"NSString")
    if i < 0:
        return None
    j = blob.find(b"+", i + 8, i + 16)
    if j < 0:
        return None
    j += 1
    n = blob[j]
    j += 1
    if n == 0x81:
        n, j = int.from_bytes(blob[j:j + 2], "little"), j + 2
    elif n == 0x82:
        n, j = int.from_bytes(blob[j:j + 4], "little"), j + 4
    return blob[j:j + n].decode("utf-8", "replace")


FILE_MARK = b"__kIMFileTransferGUIDAttributeName"
GUID_RE = re.compile(rb"(?:at_\d+_)?[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}")


def file_guids(blob):
    """The file transfers a message's body points at: one per attachment it shows."""
    blob = bytes(blob or b"")
    found, at = [], blob.find(FILE_MARK)
    while at >= 0:
        m = GUID_RE.search(blob, at + len(FILE_MARK), at + len(FILE_MARK) + 80)
        if m:
            found.append(m.group().decode())
        at = blob.find(FILE_MARK, at + 1)
    return found


def words(row):
    text = row["text"] if row["text"] is not None else decode_body(row["attributedBody"])
    return (text or "").replace("￼", "").strip() or None


def same_words(a, b):
    squash = lambda s: re.sub(r"\s+", " ", (s or "").replace("￼", "")).strip()
    return squash(a) == squash(b)


TRANSFER = {-1: "archiving", 0: "waiting", 1: "accepted", 2: "preparing", 3: "transferring", 4: "finalizing",
            5: "transferred", 6: "failed", 7: "failed"}

MESSAGE_COLUMNS = ("m.ROWID AS rowid, m.guid, m.text, m.attributedBody, m.service, m.is_from_me, m.is_sent, "
                   "m.is_delivered, m.is_read, m.error, m.date, m.date_delivered, m.date_read, m.cache_has_attachments, "
                   "m.item_type, m.associated_message_type, h.id AS sender")


def attachments_of(con, rowid):
    return con.execute(
        "SELECT a.guid, a.transfer_name, a.total_bytes, a.mime_type, a.transfer_state, a.is_outgoing, a.filename "
        "FROM attachment a JOIN message_attachment_join j ON j.attachment_id = a.ROWID WHERE j.message_id = ? ORDER BY a.ROWID",
        (rowid,)).fetchall()


def describe(con, row, kind=None, now_ms=None):
    """One message as it stands: who, when, its words or files, and sent, delivered, read or failed."""
    now_ms = now_ms or int(time.time() * 1000)
    sent_ms = apple_ms(row["date"])
    files = []
    for a in attachments_of(con, row["rowid"]):
        files.append({"name": a["transfer_name"], "bytes": a["total_bytes"], "type": a["mime_type"],
                      "state": TRANSFER.get(a["transfer_state"], f"state {a['transfer_state']}")})
    marked = file_guids(row["attributedBody"])
    if kind == "file" or marked or row["cache_has_attachments"]:
        kind = "file"
    out = {"kind": kind or "text", "guid": row["guid"], "from_me": bool(row["is_from_me"]), "service": row["service"],
           "at": iso(sent_ms), "text": words(row), "files": files, "error": row["error"] or 0}
    if not row["is_from_me"]:
        out["from"] = row["sender"]
        out["read_by_user"] = bool(row["is_read"])
        out["status"] = "received"
        return out
    out["delivered_at"] = iso(apple_ms(row["date_delivered"]))
    out["read_at"] = iso(apple_ms(row["date_read"]))
    if row["error"]:
        out["status"] = "failed"
        out["why"] = f"Messages marked it Not Delivered (error {row['error']})"
    elif kind == "file" and not files and now_ms - (sent_ms or now_ms) > FILE_GRACE_S * 1000:
        out["status"] = "failed"
        out["why"] = ("Messages sent an empty attachment bubble and never took the file "
                      "(usually a file outside the folders Messages may read)")
    elif any(f["state"] == "failed" for f in files):
        out["status"] = "failed"
        out["why"] = "the file's transfer failed"
    elif row["date_read"]:
        out["status"] = "read"
    elif row["is_delivered"] and (kind != "file" or (files and all(f["state"] == "transferred" for f in files))):
        out["status"] = "delivered"
    elif row["is_sent"]:
        out["status"] = "sent"
    else:
        out["status"] = "sending"
    return out


# Who's who

_contacts = {"at": 0, "names": {}, "people": {}}


def handle_key(handle):
    """A number or email as Messages writes a handle: +15550101234, or the email in lowercase."""
    s = (handle or "").strip()
    if "@" in s:
        return s.lower()
    digits = re.sub(r"\D", "", s)
    if s.startswith("+") and len(digits) >= 8:
        return "+" + digits
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if 3 <= len(digits) <= 6:
        return digits
    raise ValueError(f"{handle!r} isn't a phone number or an email")


def load_contacts():
    """Contacts' names by number and email, and each name's numbers and emails, read again every 10 minutes."""
    if time.time() - _contacts["at"] <= 600:
        return
    names, people = {}, {}
    for path in glob.glob(os.path.join(CONTACTS, "Sources", "*", "AddressBook-v22.abcddb")) + \
            [os.path.join(CONTACTS, "AddressBook-v22.abcddb")]:
        try:
            con = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
            person = ("TRIM(COALESCE(r.ZFIRSTNAME, '') || ' ' || COALESCE(r.ZLASTNAME, '')), r.ZORGANIZATION, r.ZNICKNAME")
            rows = []
            for name, org, nick, number in con.execute(
                    f"SELECT {person}, p.ZFULLNUMBER FROM ZABCDPHONENUMBER p JOIN ZABCDRECORD r ON r.Z_PK = p.ZOWNER"):
                try:
                    rows.append((name, org, nick, handle_key(number)))
                except ValueError:
                    pass
            for name, org, nick, email in con.execute(
                    f"SELECT {person}, e.ZADDRESS FROM ZABCDEMAILADDRESS e JOIN ZABCDRECORD r ON r.Z_PK = e.ZOWNER"):
                if email:
                    rows.append((name, org, nick, email.strip().lower()))
            con.close()
        except sqlite3.Error:
            continue
        for name, org, nick, key in rows:
            shown = name or nick or org
            if shown:
                names.setdefault(key, shown)
                entry = people.setdefault(shown.lower(), {"name": shown, "nick": (nick or "").lower(), "handles": set()})
                entry["handles"].add(key)
    _contacts.update(at=time.time(), names=names, people=people)


def named(query):
    """Everyone in Contacts whose name holds every word of `query`, each starting one of theirs; the
    one whose name is exactly that when there is one."""
    load_contacts()
    want = re.findall(r"[\w'-]+", query.lower())
    if not want:
        return []
    fits = [p for p in _contacts["people"].values()
            if all(any(t.startswith(w) for t in re.findall(r"[\w'-]+", f"{p['name']} {p['nick']}".lower())) for w in want)]
    exact = [p for p in fits if p["name"].lower() == " ".join(want)]
    return exact if len(exact) == 1 else fits


def contact_name(handle):
    """The name Contacts has for a number or email, if any."""
    load_contacts()
    try:
        return _contacts["names"].get(handle_key(handle))
    except ValueError:
        return None


def person_entity(handle):
    """The person: entity the macOS appendage puts its messages about: person:<digits>."""
    if not handle or "@" in handle:
        return None
    digits = re.sub(r"\D", "", handle)
    return f"person:{digits[1:] if handle.startswith('+1') and len(digits) == 11 else digits}"


def resolve(to, service="auto"):
    """Where a text to `to` goes: an existing conversation (by its guid), or a person by number or
    email (their conversation if there is one, else a new one), or a group chat by its name."""
    service = (service or "auto").lower()
    if service not in ("auto", "imessage", "sms"):
        raise ValueError("service is auto, imessage or sms")
    t = (to or "").strip()
    if not t:
        raise ValueError("say who it's to: a number, an email, a chat's guid or a group chat's name")
    con = connect()
    try:
        if ";" in t:
            chat = con.execute("SELECT guid, chat_identifier, display_name, style FROM chat WHERE guid = ?", (t,)).fetchone()
            if not chat:
                raise LookupError(f"Messages has no conversation {t}")
            return _dest(con, chat, None, "chat")
        try:
            handle = handle_key(t)
        except ValueError:
            chats = con.execute("SELECT guid, chat_identifier, display_name, style FROM chat WHERE lower(display_name) = lower(?)",
                                (t,)).fetchall()
            if len(chats) == 1:
                return _dest(con, chats[0], None, "chat")
            if chats:
                raise LookupError(f"{len(chats)} group chats are called {t!r}: give one's guid ({', '.join(c['guid'] for c in chats)})")
            raise ValueError(f"{t!r} isn't a phone number, an email, a chat's guid or a group chat's name")
        chats = con.execute(
            "SELECT c.guid, c.chat_identifier, c.display_name, c.style, MAX(j.message_date) AS last FROM chat c "
            "LEFT JOIN chat_message_join j ON j.chat_id = c.ROWID WHERE c.chat_identifier = ? AND c.style = 45 "
            "GROUP BY c.ROWID ORDER BY (c.guid LIKE 'any;%') DESC, last DESC", (handle,)).fetchall()
        if chats and service == "auto":
            return _dest(con, chats[0], handle, "chat")
        mode = "sms" if service == "sms" else "imessage"
        return _dest(con, chats[0] if chats else None, handle, mode)
    finally:
        con.close()


def _dest(con, chat, handle, mode):
    group = bool(chat and chat["style"] == 43)
    handle = handle or (None if group else chat["chat_identifier"])
    last = None
    if chat:
        last = con.execute(
            "SELECT m.service FROM message m JOIN chat_message_join j ON j.message_id = m.ROWID JOIN chat c ON c.ROWID = j.chat_id "
            "WHERE c.guid = ? AND m.item_type = 0 ORDER BY m.date DESC LIMIT 1", (chat["guid"],)).fetchone()
    name = (chat["display_name"] if chat and chat["display_name"] else None) or (contact_name(handle) if handle else None)
    return {"mode": mode, "target": chat["guid"] if mode == "chat" else handle, "chat": chat["guid"] if chat else None,
            "handle": handle, "group": group, "name": name, "new_conversation": chat is None,
            "last_service": last["service"] if last else None}


def label(dest):
    shown = pretty(dest["handle"]) if dest.get("handle") else dest.get("chat")
    return f"{dest['name']} ({shown})" if dest.get("name") and shown else (dest.get("name") or shown)


def pretty(handle):
    if handle and re.fullmatch(r"\+1\d{10}", handle):
        return f"({handle[2:5]}) {handle[5:8]}-{handle[8:]}"
    return handle


# Files

def stage(spec):
    """Puts a file where Messages may read it: a path on this Mac, a blob (sha256:…), an https URL, or
    {path | blob | url, name}. Answers its path there, name, size and hash."""
    if isinstance(spec, dict):
        source, name = spec.get("path") or spec.get("blob") or spec.get("url"), spec.get("name")
    else:
        source, name = spec, None
    if not source or not isinstance(source, str):
        raise ValueError(f"a file is a path, a blob or a URL, not {spec!r}")
    folder = os.path.join(STAGE, uuid.uuid4().hex[:12])
    os.makedirs(folder, exist_ok=True)
    try:
        if source.startswith("sha256:"):
            name = name or source[7:19]
            dest = os.path.join(folder, safe_name(name))
            fetch_blob(source, dest)
        elif re.match(r"https?://", source):
            name = name or os.path.basename(source.split("?")[0]) or "file"
            dest = os.path.join(folder, safe_name(name))
            download(source, dest)
        else:
            path = os.path.expanduser(source)
            if not os.path.isfile(path):
                raise FileNotFoundError(f"there's no file at {source} on this Mac")
            name = name or os.path.basename(path)
            dest = os.path.join(folder, safe_name(name))
            shutil.copyfile(path, dest)
        size = os.path.getsize(dest)
        if size == 0:
            raise ValueError(f"{name} is empty")
        if size > MAX_FILE_BYTES:
            raise ValueError(f"{name} is {size // 1_000_000} MB: Messages sends at most 100 MB")
        with open(dest, "rb") as f:
            digest = hashlib.sha256(f.read()).hexdigest()
        return {"name": os.path.basename(dest), "path": dest, "bytes": size, "sha256": digest, "source": source}
    except Exception:
        shutil.rmtree(folder, ignore_errors=True)
        raise


def safe_name(name):
    name = os.path.basename(name).strip() or "file"
    return re.sub(r"[/\x00:]", "-", name)


def sweep_staged(keep_s=3 * 86400):
    """Takes away staged files once their sends have settled (Messages keeps its own copy), and any
    left more than three days."""
    if not os.path.isdir(STAGE):
        return
    live = {os.path.dirname(f["path"]) for s in SENDS.values() if s.get("watch") for f in s.get("files", [])}
    for folder in glob.glob(os.path.join(STAGE, "*")):
        try:
            age = time.time() - os.path.getmtime(folder)
        except OSError:
            continue
        if (folder not in live and age > 600) or age > keep_s:
            shutil.rmtree(folder, ignore_errors=True)


# Sending

SEND_SCRIPT = """on run argv
	set theMode to item 1 of argv
	set theTarget to item 2 of argv
	set thePart to item 3 of argv
	set thePayload to item 4 of argv
	set dryRun to ((count of argv) > 4)
	if dryRun then set dryRun to (item 5 of argv is "dry")
	if thePart is "file" then set thePayload to (thePayload as POSIX file)
	with timeout of 30 seconds
		tell application "Messages"
			if theMode is "chat" then
				set dest to chat id theTarget
			else if theMode is "sms" then
				set dest to participant theTarget of (1st account whose service type = SMS)
			else
				set dest to participant theTarget of (1st account whose service type = iMessage)
			end if
			if dryRun then
				if theMode is "chat" then return "chat " & (id of dest)
				return "participant " & (handle of dest)
			end if
			send thePayload to dest
		end tell
	end timeout
	return "sent"
end run
"""
"""Sends one part (a text, or a file by its path) to a chat by its id, or to a person over iMessage or
SMS; with "dry" after, finds where it would go and stops short of sending. (Its variables avoid
words Messages' dictionary owns, like kind.)"""

ACCOUNTS_SCRIPT = """with timeout of 15 seconds
	tell application "Messages"
		set out to {}
		repeat with svc in {"iMessage", "SMS", "RCS"}
			set found to "none"
			try
				if contents of svc is "iMessage" then
					set a to 1st account whose service type = iMessage
				else if contents of svc is "SMS" then
					set a to 1st account whose service type = SMS
				else
					set a to 1st account whose service type = RCS
				end if
				set found to (connection status of a as text)
				if not (enabled of a) then set found to "off"
			end try
			set end of out to (contents of svc) & " " & found
		end repeat
	end tell
end timeout
set AppleScript's text item delimiters to ", "
return out as text
"""
"""Messages' accounts and whether each is connected (Tahoe refuses `service type of every account`)."""

SCRIPT_ERRORS = {
    "-1712": ("Messages didn't answer within 30 s. Usually a macOS prompt is waiting on the Mac mini's screen "
              "(device:local facts.prompts_waiting) or Messages is stuck; nothing was sent"),
    "-1743": ("macOS doesn't let this Mac's Switchboard appendage drive Messages (Automation is off for it): "
              "request_access automation on macos@local, and the user turns Messages on"),
    "-1728": "Messages couldn't find that conversation or person",
    "-1719": "Messages couldn't find that conversation or person",
}


def script_error(err):
    for code, why in SCRIPT_ERRORS.items():
        if code in err:
            return f"{why} ({err})"
    return f"Messages refused it: {err}" if err else "osascript failed without saying why"


def new_key():
    return f"t{int(time.time() * 1000):x}{uuid.uuid4().hex[:6]}"


def entity_of(key):
    return "text-send:" + re.sub(r"[^a-z0-9-]+", "-", key.lower()).strip("-")


def max_rowid():
    con = connect()
    try:
        return con.execute("SELECT COALESCE(MAX(ROWID), 0) FROM message").fetchone()[0]
    finally:
        con.close()


def find_row(dest, after, kind, text, taken, name=None, within=10.0):
    """The message row Messages made for a part just sent: from me, in that conversation, after
    `after`, its words matching (a text) or showing a file (a file)."""
    deadline = time.time() + within
    while True:
        con = connect()
        try:
            rows = con.execute(
                f"SELECT DISTINCT {MESSAGE_COLUMNS} FROM message m JOIN chat_message_join j ON j.message_id = m.ROWID "
                "JOIN chat c ON c.ROWID = j.chat_id LEFT JOIN handle h ON h.ROWID = m.handle_id "
                "WHERE m.ROWID > ? AND m.is_from_me = 1 AND m.item_type = 0 AND m.associated_message_type = 0 "
                "AND (c.guid = ? OR (? IS NOT NULL AND c.chat_identifier = ? AND c.style = 45)) ORDER BY m.ROWID",
                (after, dest.get("chat") or dest["target"], dest.get("handle"), dest.get("handle"))).fetchall()
            rows = [r for r in rows if r["rowid"] not in taken]
            if kind == "text":
                plain = [r for r in rows if not file_guids(r["attributedBody"]) and not r["cache_has_attachments"]]
                for r in plain:
                    if same_words(words(r), text):
                        return r, "words"
                if plain and time.time() >= deadline - within / 2:
                    return plain[0], "order"
            else:
                files = [r for r in rows if file_guids(r["attributedBody"]) or r["cache_has_attachments"]]
                for r in files:
                    if any(a["transfer_name"] == name for a in attachments_of(con, r["rowid"])):
                        return r, "name"
                if files and time.time() >= deadline - within / 2:
                    return files[0], "order"
        finally:
            con.close()
        if time.time() >= deadline:
            return None, None
        time.sleep(0.4)


RANK = {"failed": 0, "not_sent": 0, "sending": 1, "sent": 2, "delivered": 3, "read": 4}


def evaluate(send):
    """Reads where each part of a send stands now, and the send as a whole."""
    now_ms = int(time.time() * 1000)
    con = connect()
    try:
        for part in send["parts"]:
            if not part.get("rowid"):
                continue
            row = con.execute(f"SELECT {MESSAGE_COLUMNS} FROM message m LEFT JOIN handle h ON h.ROWID = m.handle_id "
                              "WHERE m.ROWID = ?", (part["rowid"],)).fetchone()
            if not row:
                part.update(status="failed", why="its message is gone from Messages (deleted?)")
                continue
            seen = describe(con, row, part["kind"], now_ms)
            for k in ("status", "why", "service", "at", "delivered_at", "read_at", "error", "files", "text", "guid"):
                if seen.get(k) is not None or k in ("why",):
                    part[k] = seen.get(k)
    finally:
        con.close()
    statuses = [p["status"] for p in send["parts"]]
    overall = min(statuses, key=lambda s: RANK[s]) if statuses else "not_sent"
    if overall == "not_sent" and any(RANK[s] >= RANK["sent"] for s in statuses):
        overall = "failed"
    services = [p.get("service") for p in send["parts"] if p.get("service")]
    send["service"] = services[0] if services else send.get("service")
    send["status"] = overall
    send["sent_at"] = next((p.get("at") for p in send["parts"] if p.get("at")), None)
    finished = [p.get("delivered_at") for p in send["parts"]]
    send["delivered_at"] = max(finished) if finished and all(finished) else None
    reads = [p.get("read_at") for p in send["parts"] if p.get("read_at")]
    send["read_at"] = max(reads) if reads else None
    send["summary"] = summarize(send)
    return send


def summarize(send):
    to = send.get("label") or send.get("to")
    what = []
    if any(p["kind"] == "text" for p in send["parts"]):
        what.append("the text")
    names = [p["name"] for p in send["parts"] if p["kind"] == "file"]
    what += names
    things = " and ".join(what) if len(what) < 3 else ", ".join(what[:-1]) + " and " + what[-1]
    over = f" over {send['service']}" if send.get("service") else ""
    status = send["status"]
    if status == "read":
        return f"Read by {to}{when(send['read_at'])}: {things}{over}."
    if status == "delivered":
        return f"Delivered to {to}{over}{when(send['delivered_at'])}: {things}."
    if status == "sent":
        if send.get("service") == "SMS":
            return f"Sent to {to} as SMS{when(send['sent_at'])}: {things}. SMS gives no delivery receipt, and no error came back."
        watching = " I'm watching for its receipt." if send.get("watch") else ""
        return f"Sent to {to}{over}{when(send['sent_at'])}, not delivered yet: {things}.{watching}"
    if status == "sending":
        return f"Still sending to {to}: Messages hasn't sent {things} yet."
    bad = [p for p in send["parts"] if p["status"] in ("failed", "not_sent")]
    good = [p for p in send["parts"] if p not in bad and RANK.get(p["status"], 0) >= RANK["sent"]]
    why = "; ".join(f"{p.get('name') or 'the text'}: {p.get('why') or p['status']}" for p in bad)
    if status == "not_sent" or not good:
        return f"Nothing reached {to}. {why[:1].upper() + why[1:]}."
    went = " and ".join(p.get("name") or "the text" for p in good)
    return f"Partly sent to {to}: {went} went, but {why}."


def when(stamp):
    """ at 11:00:49 AM PST, from an ISO time, or nothing."""
    if not stamp:
        return ""
    return f" at {local(int(datetime.fromisoformat(stamp.replace('Z', '+00:00')).timestamp() * 1000))}"


def public(send):
    """A send as its caller and the brain see it."""
    parts = []
    for p in send["parts"]:
        parts.append({k: p.get(k) for k in ("kind", "name", "bytes", "status", "why", "guid", "service", "at",
                                             "delivered_at", "read_at", "error", "files", "text") if p.get(k) not in (None, [])})
    return {"status": send["status"], "summary": send["summary"], "key": send["key"], "entity": entity_of(send["key"]),
            "to": send["to"], "name": send.get("name"), "chat": send.get("chat"), "service": send.get("service"),
            "sent_at": send.get("sent_at"), "delivered_at": send.get("delivered_at"), "read_at": send.get("read_at"),
            "parts": parts, "watching": bool(send.get("watch")),
            "check": f'entities | where id == "{entity_of(send["key"])}" and facts.status in ("delivered", "read")'}


def record(send, force=False):
    """Observes text-send:<key> in comms/text-sends when where it stands changed."""
    mark = (send["status"], send.get("read_at"), send.get("delivered_at"))
    if not force and send.get("_recorded") == list(mark):
        return
    send["_recorded"] = list(mark)
    seen = public(send)
    facts = {k: v for k, v in seen.items() if k not in ("summary", "entity", "check", "watching") and v is not None}
    facts["text"] = send.get("text") or None
    facts["files"] = [f["name"] for f in send.get("files", [])]
    facts["handle"] = send.get("handle")
    about = [f"imessage:{p['guid']}" for p in send["parts"] if p.get("guid")]
    person = person_entity(send.get("handle"))
    about += [person] if person else []
    about += [a for a in send.get("about") or [] if a not in about]
    app.emit("send", {"entity": entity_of(send["key"]), "kind": "text-send", "name": f"Text to {send.get('label') or send['to']}",
                      "summary": send["summary"], "facts": facts},
             about=about, event_id=f"{entity_of(send['key'])}-{'-'.join(str(m) for m in mark)}")


def settle(send, now=None):
    """Whether a send needs watching still."""
    now = now or time.time()
    age = now - send["started"]
    status = send["status"]
    if status in ("failed", "not_sent"):
        return False
    if status == "read":
        return False
    if status == "delivered":
        return age < READ_FOR_S
    if status == "sent" and send.get("service") == "SMS":
        return age < SMS_SETTLE_S
    return age < WATCH_FOR_S


def save():
    with STATE_LOCK:
        recent = sorted(SENDS.values(), key=lambda s: s["started"])[-300:]
        data = {"sends": {s["key"]: s for s in recent}}
        tmp = os.path.join(HOME, "state.json.tmp")
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, os.path.join(HOME, "state.json"))


def load():
    try:
        with open(os.path.join(HOME, "state.json")) as f:
            SENDS.update(json.load(f).get("sends", {}))
        for send in SENDS.values():
            send["_live"] = False  # a call cut off by a restart: the watch carries it on
        if SENDS:
            last = max(SENDS.values(), key=lambda s: s["started"])
            state["last_send"] = {"key": last["key"], "status": last.get("status"), "at": iso(int(last["started"] * 1000))}
    except (OSError, ValueError):
        pass


def fingerprint(dest, text, staged):
    h = hashlib.sha256(json.dumps([dest.get("chat") or dest["target"], re.sub(r"\s+", " ", text or "").strip(),
                                   [f["sha256"] for f in staged]]).encode())
    return h.hexdigest()[:24]


SEND_SCHEMA = {
    "type": "object",
    "properties": {
        "to": {"type": "string", "description": "Who: a phone number in any form (+1 555-010-0000, 5550100000), an email, a conversation's guid (any;-;+15550100000, any;+;chat…), or a group chat's name."},
        "text": {"type": "string", "description": "The words, exactly as approved. Line breaks are kept. Sent first, then each file."},
        "files": {"type": "array", "items": {"anyOf": [{"type": "string"}, {"type": "object"}]},
                  "description": "Files to send after the text: paths on this Mac, blobs (sha256:…), https URLs, or {path | blob | url, name}. Each goes through ~/Pictures, where Messages may read it."},
        "service": {"type": "string", "enum": ["auto", "imessage", "sms"], "description": "auto (default) uses their conversation as it is (iMessage, RCS or SMS), else iMessage; sms or imessage forces one."},
        "key": {"type": "string", "description": "Never sends twice with the same key: the approval ask's id, or the key approval gave. A second call with it answers where the first send stands."},
        "again": {"type": "boolean", "description": "Send it even though the same words and files went to the same conversation in the last 15 minutes."},
        "wait_seconds": {"type": "number", "description": "How long to wait for delivery before answering (default 20, at most 120). It watches on after, recording text-send:<key> when it lands."},
        "about": {"type": "array", "items": {"type": "string"}, "description": "Entities it's about, like lead:a-lead or ws:shop, on its record."},
        "dry_run": {"type": "boolean", "description": "Do everything but send: find who it's to, stage the files and have Messages find the conversation, then stop. Nothing is sent or recorded."},
    },
    "required": ["to"],
}


@app.tool(schema=SEND_SCHEMA, speaks_for_person=True, open_world=True)
def send(to, text="", files=None, service="auto", key=None, again=False, wait_seconds=20, about=None, dry_run=False, ctx=None):
    """Text someone from the user's Messages on the Mac mini (iMessage, RCS or SMS, as their conversation
    goes), with files if given, and answer whether it landed: each part's status (sending, sent,
    delivered, read, failed, not_sent), when, over which service, the words as Messages has them, and
    each file's transfer, with a summary to say as it is. Speaks for the user: only words he asked for
    or approved, with the approval's key. It keeps watching after it answers, and records
    text-send:<key> in comms/text-sends when it's delivered, read or fails."""
    files = files or []
    if isinstance(files, (str, dict)):
        files = [files]
    text = text or ""
    if not text.strip() and not files:
        raise ValueError("nothing to send: give text, files or both")
    if key is not None and not re.fullmatch(r"[A-Za-z0-9._:-]{1,100}", str(key)):
        raise ValueError("a key is up to 100 letters, digits and . _ : -")
    wait_seconds = max(0.0, min(float(wait_seconds if wait_seconds is not None else 20), 120.0))
    say = ctx.progress if ctx else (lambda *_: None)
    with SEND_LOCK:
        if key and key in SENDS:
            done = evaluate(SENDS[key])
            out = public(done)
            out["already"] = f"This key was sent at {done.get('sent_at') or iso(int(done['started'] * 1000))}: nothing was sent again."
            return out
        dest = resolve(to, service)
        say(f"Sending to {label(dest)}")
        staged = []
        try:
            for spec in files:
                staged.append(stage(spec))
        except Exception:
            for f in staged:
                shutil.rmtree(os.path.dirname(f["path"]), ignore_errors=True)
            raise
        fp = fingerprint(dest, text, staged)
        if dry_run:
            return rehearse(dest, text, staged)
        if not again:
            for prior in SENDS.values():
                if prior.get("fingerprint") == fp and time.time() - prior["started"] < DUPLICATE_S and prior.get("status") != "not_sent":
                    for f in staged:
                        shutil.rmtree(os.path.dirname(f["path"]), ignore_errors=True)
                    raise ValueError(f"The same words and files went to {label(dest)} at {local(int(prior['started'] * 1000))} "
                                     f"(key {prior['key']}, {prior.get('status')}): check(key=\"{prior['key']}\") says where it stands. "
                                     "Pass again: true to send it a second time.")
        key = key or new_key()
        sending = {"key": key, "to": to, "handle": dest.get("handle"), "name": dest.get("name"), "label": label(dest),
                   "chat": dest.get("chat"), "mode": dest["mode"], "text": text, "files": staged, "fingerprint": fp,
                   "started": time.time(), "about": about or [], "watch": True, "_live": True, "parts": []}
        if text.strip():
            sending["parts"].append({"kind": "text", "status": "not_sent"})
        sending["parts"] += [{"kind": "file", "name": f["name"], "bytes": f["bytes"], "path": f["path"], "status": "not_sent"} for f in staged]
        SENDS[key] = sending
        state["sends"] += 1
        after, taken, stop = max_rowid(), set(), None
        for part in sending["parts"]:
            if stop:
                part["why"] = f"not sent, since {stop}"
                continue
            payload = text if part["kind"] == "text" else part["path"]
            code, _, err = osascript(SEND_SCRIPT, [dest["mode"], dest["target"], part["kind"], payload])
            if code != 0:
                part["why"] = script_error(err)
                stop = f"{'the text' if part['kind'] == 'text' else part['name']} failed"
                state["last_failure"], state["last_failure_at"] = part["why"], int(time.time() * 1000)
                continue
            row, how = find_row(dest, after, part["kind"], text, taken, part.get("name"))
            if row is None:
                part["status"], part["why"] = "failed", "Messages took it but no message appeared in the conversation within 10 s"
                continue
            taken.add(row["rowid"])
            part.update(rowid=row["rowid"], guid=row["guid"], matched_by=how, status="sending")
            if not sending.get("chat"):
                sending["chat"] = chat_of(row["rowid"])
            if part is not sending["parts"][-1]:
                time.sleep(1)
        evaluate(sending)
        save()
    deadline = time.time() + wait_seconds
    while RANK.get(sending["status"], 0) in (RANK["sending"], RANK["sent"]) and time.time() < deadline:
        if ctx and ctx.cancelled.is_set():
            break
        if sending["status"] == "sent" and sending.get("service") == "SMS":
            break
        time.sleep(0.5)
        evaluate(sending)
        if sending["status"] == "sent" and not sending.get("_said"):
            sending["_said"] = True
            say("Sent; waiting for its delivery receipt")
    sending["watch"] = settle(sending)
    sending["_live"] = False
    if sending["status"] in ("failed", "not_sent"):
        state["failed"] += 1
    state["last_send"] = {"key": key, "status": sending["status"], "at": iso(int(sending["started"] * 1000))}
    record(sending)
    save()
    return public(sending)


def rehearse(dest, text, staged):
    """A send's dry run: Messages finds where each part would go, and nothing is sent."""
    parts = ([{"kind": "text", "payload": text}] if text.strip() else []) + \
        [{"kind": "file", "name": f["name"], "bytes": f["bytes"], "payload": f["path"]} for f in staged]
    try:
        for part in parts:
            code, said, err = osascript(SEND_SCRIPT, [dest["mode"], dest["target"], part["kind"], part.pop("payload"), "dry"])
            part["would_go"] = said if code == 0 else None
            if code != 0:
                part["why"] = script_error(err)
    finally:
        for f in staged:
            shutil.rmtree(os.path.dirname(f["path"]), ignore_errors=True)
    ok = all(p.get("would_go") for p in parts)
    how = "an existing conversation" if dest["mode"] == "chat" else f"a new {'SMS' if dest['mode'] == 'sms' else 'iMessage'} conversation"
    return {"dry_run": True, "ok": ok, "to": label(dest), "chat": dest.get("chat"), "last_service": dest.get("last_service"),
            "parts": parts,
            "summary": (f"Ready: it would go to {label(dest)} in {how}. Nothing was sent." if ok else
                        f"It couldn't go to {label(dest)}: " + "; ".join(p["why"] for p in parts if p.get("why")) + ". Nothing was sent.")}


def chat_of(rowid):
    con = connect()
    try:
        row = con.execute("SELECT c.guid FROM chat c JOIN chat_message_join j ON j.chat_id = c.ROWID WHERE j.message_id = ?",
                          (rowid,)).fetchone()
        return row["guid"] if row else None
    finally:
        con.close()


# Looking

CHECK_SCHEMA = {
    "type": "object",
    "properties": {
        "key": {"type": "string", "description": "A send's key (what send answered): where it stands now."},
        "guid": {"type": "string", "description": "Or one message's guid (from either side, like a comms/imessage record's)."},
        "to": {"type": "string", "description": "Or a conversation (a number, an email, a guid or a group chat's name), or a person's name in Contacts (every chat with any of their numbers and emails; several who fit come back as candidates): the latest messages, both sides, oldest first, with each one's words, files and status."},
        "limit": {"type": "integer", "description": "How many (default 10, at most 50)."},
    },
}


def public_message(con, row):
    """Verification message bodies never enter public tool results or events. A message with no
    words (a file, a sticker) has none to hide."""
    d = describe(con, row)
    if d["text"] and re.search(r"(?:verification|security|sign[ -]?in|login|one[ -]?time|authentication)\s+(?:code|password)|(?:code|password)\s+(?:for|to)\s+(?:sign|log)", d["text"], re.I):
        d["text"] = "Private authentication message"
    return d


def person_messages(con, person, limit):
    """Someone's latest messages in every chat with one of their numbers or emails, both sides, oldest first."""
    handles = sorted(person["handles"])
    marks = ", ".join("?" for _ in handles)
    rows = con.execute(
        f"SELECT {MESSAGE_COLUMNS} FROM message m JOIN chat_message_join j ON j.message_id = m.ROWID "
        "JOIN chat c ON c.ROWID = j.chat_id LEFT JOIN handle h ON h.ROWID = m.handle_id "
        f"WHERE c.chat_identifier IN ({marks}) AND c.style = 45 AND m.item_type = 0 AND m.associated_message_type = 0 "
        "ORDER BY m.date DESC LIMIT ?", (*handles, limit)).fetchall()
    return {"to": person["name"], "handles": handles, "messages": [public_message(con, r) for r in reversed(rows)]}


@app.tool(schema=CHECK_SCHEMA, read_only=True)
def check(key="", guid="", to="", limit=10):
    """Where texts stand now, read from Messages: a send by its key, one message by its guid, or a
    conversation's latest messages (each with its words, even those only in attributedBody, its
    files and their transfer, and sent, delivered, read or failed). With nothing, the latest sends
    this tool made."""
    limit = max(1, min(int(limit or 10), 50))
    if key:
        if key not in SENDS:
            raise LookupError(f"no send with key {key} (sends are kept 300 deep)")
        with SEND_LOCK:
            send = evaluate(SENDS[key])
            record(send)
        return public(send)
    con = connect()
    try:
        if guid:
            row = con.execute(f"SELECT {MESSAGE_COLUMNS} FROM message m LEFT JOIN handle h ON h.ROWID = m.handle_id WHERE m.guid = ?",
                              (guid,)).fetchone()
            if not row:
                raise LookupError(f"Messages has no message {guid}")
            return public_message(con, row)
        if to:
            try:
                dest = resolve(to)
            except ValueError:
                people = named(to)
                if not people:
                    raise
                if len(people) > 1:
                    raise LookupError(f"{len(people)} people in Contacts fit {to!r}: " + "; ".join(
                        f"{p['name']} ({', '.join(sorted(p['handles']))})" for p in people[:8]))
                return person_messages(con, people[0], limit)
            if not dest.get("chat"):
                return {"to": label(dest), "messages": [], "note": "There's no conversation with them in Messages yet."}
            rows = con.execute(
                f"SELECT {MESSAGE_COLUMNS} FROM message m JOIN chat_message_join j ON j.message_id = m.ROWID "
                "JOIN chat c ON c.ROWID = j.chat_id LEFT JOIN handle h ON h.ROWID = m.handle_id "
                "WHERE c.guid = ? AND m.item_type = 0 AND m.associated_message_type = 0 ORDER BY m.date DESC LIMIT ?",
                (dest["chat"], limit)).fetchall()
            return {"to": label(dest), "chat": dest["chat"], "last_service": dest.get("last_service"),
                    "messages": [public_message(con, r) for r in reversed(rows)]}
    finally:
        con.close()
    latest = sorted(SENDS.values(), key=lambda s: s["started"], reverse=True)[:limit]
    return {"sends": [{k: v for k, v in public(s).items() if k in ("key", "status", "summary", "to", "sent_at")} for s in latest]}


@app.tool(read_only=True, schema={
    "type": "object",
    "properties": {
        "to": SEND_SCHEMA["properties"]["to"],
        "text": {"type": "string", "description": "The words to put on the card for the user to edit and approve."},
        "files": SEND_SCHEMA["properties"]["files"],
        "service": SEND_SCHEMA["properties"]["service"],
        "label": {"type": "string", "description": "The line over the draft (default: Text to <name> (<number>))."},
        "about": SEND_SCHEMA["properties"]["about"],
    },
    "required": ["to", "text"],
})
def approval(to, text, files=None, service="auto", label=None, about=None):
    """One approval card's parts for a text, in one step, sending nothing: checks who it's to (their
    conversation, its service, their name in Contacts) and that each file is there, and answers
    drafts, assists (the Send that Approve & send runs, with the user's words as {{draft:text}} and a
    key so it never goes twice), blobs (the files, for the card) and resolve_when (it closes once
    it's sent) to put on the brain's ask as they are."""
    files = files or []
    if isinstance(files, (str, dict)):
        files = [files]
    dest = resolve(to, service)
    blobs, args_files, warnings = [], [], []
    for spec in files:
        f = stage(spec)
        try:
            mime = mimetypes.guess_type(f["name"])[0] or "application/octet-stream"
            source = f["source"]
            blob = source if source.startswith("sha256:") else put_blob(f["path"], mime)
            blobs.append({"blob": blob, "type": mime, "name": f["name"]})
            args_files.append({"blob": blob, "name": f["name"]})
        finally:
            shutil.rmtree(os.path.dirname(f["path"]), ignore_errors=True)
    if dest["new_conversation"]:
        warnings.append(f"There's no conversation with {label_or(dest)} in Messages yet: it starts one over {'SMS' if dest['mode'] == 'sms' else 'iMessage'}.")
    if dest.get("last_service") == "SMS" and files:
        warnings.append("Their conversation is SMS: files go as MMS, which carriers shrink or drop.")
    key = new_key()
    args = {"to": dest["chat"] or dest["target"], "text": "{{draft:text}}", "key": key}
    if args_files:
        args["files"] = args_files
    if service != "auto":
        args["service"] = service
    if about:
        args["about"] = about
    return {
        "to": label_or(dest), "chat": dest.get("chat"), "last_service": dest.get("last_service"),
        "drafts": [{"name": "text", "label": label or f"Text to {label_or(dest)}", "text": text}],
        "assists": [{"label": "Send", "target": INSTANCE, "action": "send", "args": args}],
        "blobs": blobs,
        "resolve_when": f'entities | where id == "{entity_of(key)}" and facts.status in ("sent", "delivered", "read")',
        "key": key, "entity": entity_of(key), "warnings": warnings,
    }


def label_or(dest):
    return label(dest) or dest.get("target")


@app.tool(read_only=True, schema={
    "type": "object",
    "properties": {"probe": {"type": "boolean", "description": "Also ask Messages for its accounts through AppleScript (up to 15 s), which proves Automation works."}},
})
def status(probe=False):
    """Whether it can text: Messages' database readable and its latest message, Messages running,
    Contacts, the sends it's watching and its last one; with probe, Messages' accounts as AppleScript
    sees them (proving it may drive Messages)."""
    out = dict(state)
    try:
        con = connect()
        last = con.execute("SELECT MAX(date) FROM message").fetchone()[0]
        con.close()
        out["chat_db"] = {"ok": True, "latest_message_at": iso(apple_ms(last))}
    except sqlite3.Error as e:
        out["chat_db"] = {"ok": False, "error": f"{e}: this process needs Full Disk Access to read {DB}"}
    out["messages_running"] = messages_running()
    contact_name("+10000000000")
    out["contacts"] = len(_contacts["names"])
    out["watching"] = sorted(k for k, s in SENDS.items() if s.get("watch"))
    out["staged"] = len(glob.glob(os.path.join(STAGE, "*")))
    if probe:
        code, said, err = osascript(ACCOUNTS_SCRIPT, [], timeout=20)
        out["automation"] = {"ok": code == 0, "accounts": said} if code == 0 else {"ok": False, "error": script_error(err)}
    return out


# Watching

def watch_once(now=None):
    """Looks again at each send still being watched, records what changed, and stops watching the
    settled ones. Answers what failed, if anything."""
    now = now or time.time()
    changed, trouble = False, None
    for send in list(SENDS.values()):
        if not send.get("watch") or (send.get("_live") and now - send["started"] < 600):
            continue  # a send still in its call is the call's to record (unless the call died)
        age = now - send["started"]
        every = 3 if age < 120 else 20 if age < 1800 else 120 if age < 6 * 3600 else 600
        if now - send.get("_looked", 0) < every:
            continue
        send["_looked"] = now
        with SEND_LOCK:
            try:
                evaluate(send)
            except sqlite3.Error as e:
                trouble = f"chat.db: {e}"
                continue
            send["watch"] = settle(send, now)
            record(send)
            changed = True
    if changed:
        save()
    sweep_staged()
    return trouble


# Messages, both ways: in Switch, this sense records every message Messages gets or sends (in
# Switchboard, the macOS appendage did).

MESSAGES = os.path.join(HOME, "messages.json")
BACKFILL = int(os.environ.get("TEXTS_BACKFILL") or 40)
"""How many of the latest messages it records the first time it runs."""
UNREAD_FOR_S = 48 * 3600
READ_EVERY_S = 30
SKIPPED_KEPT = 50
seen = {"rowid": None, "unread": {}, "read_at": 0, "skipped": {}}
"""`skipped`: each message it couldn't record, by guid: its row, when it was sent, and why. Tried
again each time it starts, so a fix records them; until then they're its failure."""
retried = {"done": False}


def load_seen():
    try:
        with open(MESSAGES) as f:
            seen.update(json.load(f))
    except (OSError, ValueError):
        pass
    seen.setdefault("skipped", {})


def save_seen():
    with open(MESSAGES + ".new", "w") as f:
        json.dump(seen, f)
    os.replace(MESSAGES + ".new", MESSAGES)


MESSAGE_ROWS = (f"SELECT {MESSAGE_COLUMNS}, c.guid AS chat FROM message m "
                "LEFT JOIN handle h ON h.ROWID = m.handle_id "
                "LEFT JOIN chat_message_join j ON j.message_id = m.ROWID LEFT JOIN chat c ON c.ROWID = j.chat_id ")


def message_body(con, row):
    """One message as the record of it: who, which chat, its words and files, and whether the user has read it."""
    d = public_message(con, row)
    who = d.get("from")
    name = contact_name(who) if who else None
    facts = {"guid": row["guid"], "chat": row["chat"], "from_me": d["from_me"], "from": who, "name": name,
             "text": d["text"], "files": d["files"], "service": d["service"], "at": d["at"],
             "read_by_user": d.get("read_by_user", True)}
    who_said = "The user" if d["from_me"] else (name or (pretty(who) if who else "Someone"))
    return {"entity": f"imessage:{row['guid']}", "summary": f"{who_said}: {d['text'] or 'File'}"[:200], "facts": facts}


def sender_of(facts):
    """Who sent it, as Switch keys people: an E.164 number or an email; a short code is a business's machine."""
    if facts["from_me"]:
        return {"handle": "me", "via": "phone", "me": True}
    who = facts["from"] or ""
    try:
        handle = handle_key(who)
    except ValueError:
        handle = who
    sender = {"handle": handle, "via": "email" if "@" in handle else "phone"}
    if facts["name"]:
        sender["name"] = facts["name"]
    if handle.isdigit() and len(handle) <= 6:
        sender["agent"] = True
    return sender


def emit_message(con, row):
    """Records one message, at the time it was sent (or, once read, when the user read it), so one
    recorded late says when it happened."""
    body = message_body(con, row)
    facts = body["facts"]
    read = facts["read_by_user"]
    sent_ms = apple_ms(row["date"])
    message = {"id": body["entity"], "conversation": f"texts:{facts['chat'] or facts['from']}",
               "text": facts["text"] or ("File" if facts["files"] else ""), "at": sent_ms,
               "unread": not facts["from_me"] and not read, "channel": "texts"}
    read_ms = apple_ms(row["date_read"]) if read and not facts["from_me"] else None
    app.emit("message", body, about=[body["entity"]], id=f"imessage:{row['guid']}:{'read' if read else 'new'}",
             sender=sender_of(facts), message=message, at=read_ms or sent_ms)
    return read


def skip(row, e):
    """A message it couldn't record: kept with why, so the rest go on and it's tried again."""
    why = f"{type(e).__name__}: {e}"
    seen["skipped"][row["guid"]] = {"rowid": row["rowid"], "at": iso(apple_ms(row["date"])), "why": why}
    while len(seen["skipped"]) > SKIPPED_KEPT:
        seen["skipped"].pop(min(seen["skipped"], key=lambda g: seen["skipped"][g]["rowid"]))
    app.log(f"skipped message {row['guid']} (row {row['rowid']}): {why}")


def retry_skipped(con):
    """The messages skipped before, tried again once a run: what records now leaves the list."""
    if not seen["skipped"]:
        return
    rowids = [s["rowid"] for s in seen["skipped"].values()]
    marks = ",".join("?" * len(rowids))
    rows = con.execute(MESSAGE_ROWS + f"WHERE m.ROWID IN ({marks}) ORDER BY m.ROWID", rowids).fetchall()
    found = {row["rowid"] for row in rows}
    for g in [g for g, s in seen["skipped"].items() if s["rowid"] not in found]:
        seen["skipped"].pop(g)  # gone from Messages: nothing left to record
    for row in rows:
        try:
            emit_message(con, row)
        except Exception as e:
            skip(row, e)
            continue
        seen["skipped"].pop(row["guid"], None)
        app.log(f"recorded message {row['guid']} (row {row['rowid']}), skipped before")
    save_seen()


MESSAGE_PAGE = 200
"""How many messages one look reads at most; the rest wait for the next."""


def messages_once(now=None):
    """Records each message since the last it saw, and each unread one once the user reads it. One it
    can't record is skipped (kept with why), never the end of the rest. True when it read every message
    chat.db had."""
    now = now or time.time()
    con = connect()
    try:
        if not retried["done"]:
            retried["done"] = True
            retry_skipped(con)
        if seen["rowid"] is None:
            top = con.execute("SELECT max(ROWID) FROM message").fetchone()[0] or 0
            seen["rowid"] = max(0, top - BACKFILL)
        rows = con.execute(MESSAGE_ROWS + "WHERE m.ROWID > ? AND m.item_type = 0 AND m.associated_message_type = 0 "
                           "ORDER BY m.ROWID LIMIT ?", (seen["rowid"], MESSAGE_PAGE)).fetchall()
        for row in rows:
            try:
                if not emit_message(con, row) and now - (apple_ms(row["date"]) or 0) / 1000 < UNREAD_FOR_S:
                    seen["unread"][row["guid"]] = row["rowid"]
            except Exception as e:
                skip(row, e)
            seen["rowid"] = row["rowid"]
        if seen["unread"] and now - seen["read_at"] >= READ_EVERY_S:
            seen["read_at"] = now
            marks = ",".join("?" * len(seen["unread"]))
            for row in con.execute(MESSAGE_ROWS + f"WHERE m.ROWID IN ({marks})", list(seen["unread"].values())).fetchall():
                too_old = now - (apple_ms(row["date"]) or 0) / 1000 >= UNREAD_FOR_S
                if row["is_read"] or too_old:
                    if row["is_read"]:
                        try:
                            emit_message(con, row)
                        except Exception as e:
                            skip(row, e)
                    seen["unread"].pop(row["guid"], None)
        if rows or seen["read_at"] == now:
            save_seen()
        return len(rows) < MESSAGE_PAGE
    finally:
        con.close()


def mark(part, trouble):
    """What a part's latest look found: a failure stays its failure until it works again."""
    if trouble:
        state["failing"][part] = trouble
        state["last_failure"], state["last_failure_at"] = f"{part}: {trouble}", int(time.time() * 1000)
    else:
        state["failing"].pop(part, None)


def failing():
    """What's failing now, in a line, or None: what Switch reads to send the agent to fix it."""
    now = [f"{part}: {why}" for part, why in state["failing"].items()]
    skipped = seen.get("skipped") or {}
    if skipped:
        latest = max(skipped.items(), key=lambda kv: kv[1]["rowid"])
        now.append(f"skipped {len(skipped)} message{'s' if len(skipped) != 1 else ''} it couldn't record "
                   f"(latest {latest[0]}: {latest[1]['why']})")
    return "; ".join(now) or None


def heartbeat():
    """Says how it is, in vitals/texts: what keeps its --health-every promise. `failing` is what's
    wrong now, cleared once it works again."""
    try:
        con = connect()
        con.execute("SELECT 1 FROM message LIMIT 1").fetchone()
        con.close()
        db_ok = True
    except sqlite3.Error as e:
        db_ok = False
        mark("chat.db", f"can't read Messages' database: {e}")
    if db_ok:
        mark("chat.db", None)
    watching = sum(1 for s in SENDS.values() if s.get("watch"))
    behind_ms = 0 if state["caught_up"] else max(0, int(time.time() * 1000) - state["caught_up_at"])
    trouble = failing()
    summary = f"Failing: {trouble}" if trouble else "Ready"
    if watching:
        summary += f"; watching {watching} send{'s' if watching != 1 else ''}"
    app.emit("heartbeat", {"entity": f"sense:{NAME}", "summary": summary[:600],
                           "facts": {"behind_ms": behind_ms, "chat_db": db_ok, "messages_running": messages_running(), "watching": watching,
                                     "sends": state["sends"], "failed": state["failed"], "last_send": state["last_send"],
                                     "failing": trouble, "skipped": seen.get("skipped") or {},
                                     "last_failure": state["last_failure"], "last_failure_at": state["last_failure_at"]}})


@app.background
def watch():
    last_beat, said = 0, None
    while True:
        try:
            trouble = watch_once()
        except Exception as e:  # one bad look shouldn't end the watch
            trouble = f"{type(e).__name__}: {e}"
        mark("watch", trouble)
        try:
            state["caught_up"] = messages_once()
            mark("messages", None)
        except Exception as e:
            state["caught_up"] = False
            mark("messages", f"{type(e).__name__}: {e}")
        if state["caught_up"]:
            state["caught_up_at"] = int(time.time() * 1000)
        if time.time() - last_beat >= HEARTBEAT_S or said != (failing(), state["caught_up"]):
            said = (failing(), state["caught_up"])
            heartbeat()
            last_beat = time.time()
        time.sleep(TICK_S)


authentication.register(app, connect, describe, MESSAGE_COLUMNS, APPLE_EPOCH)


if __name__ == "__main__":
    os.makedirs(HOME, mode=0o700, exist_ok=True)
    load()
    load_seen()
    app.run()
