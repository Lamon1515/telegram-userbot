"""
Master-plan relay: ONE userbot, multi-bot, multi web-user.

Architecture (per Lamon's master plan):
- ONE master userbot (admin logs in once via dashboard, session in Redis)
- Web users do NOT need Telegram login — identified by web session_id
- Redis tracks each web user's conversation; replies routed to the right user
- Multiple bots supported (admin adds via dashboard, users switch via tabs)

Routing:
- New bot message -> delivered to the web user awaiting reply (FIFO pending queue)
- Button click replies -> routed to the user who clicked (cb mapping stores session)
- Message edits -> updated in the session history containing that message ID

Endpoints:
- GET  /                    - Dashboard (admin: login, bots, status)
- GET  /chat                - Live chat UI (embeddable, no login needed)
- POST /api/admin/phone      - Admin step 1: send OTP
- POST /api/admin/verify     - Admin step 2: verify, save master session
- GET  /api/chat/bots        - List available bots
- POST /api/chat/send        - Web user sends message (session_id + bot + text)
- GET  /api/chat/messages    - Get messages (session_id + bot)
- POST /api/chat/callback    - Click inline button (session_id + cb_id)
- GET  /api/chat/commands    - Bot commands
- POST /api/admin/bots/add   - Add a bot
- POST /api/admin/bots/remove - Remove a bot
- GET  /api/status, /health
"""
import asyncio
import json
import logging
import os
import time

import redis
import uvicorn
from fastapi import FastAPI, Form, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pyrogram import Client, filters
from pyrogram.types import Message

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("livechat")

DEFAULT_BOTS = ["@FastDpHouseBot"]

web = FastAPI(title="Lamon Master Relay")


def get_redis():
    return redis.Redis(
        host=config.REDIS_HOST, port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD or None, db=config.REDIS_DB,
        decode_responses=True,
    )


def get_bots():
    try:
        r = get_redis()
        bots = r.smembers("relay:bots")
        if bots:
            return sorted(bots)
    except Exception:
        pass
    return DEFAULT_BOTS


def norm_bot(b):
    b = (b or "").strip()
    if b and not b.startswith("@"):
        b = "@" + b
    return b


def bot_key(b):
    return norm_bot(b).strip("@").lower()


# ------------------------------------------------------------ Account pool
# Multiple Telegram accounts: each web user gets their own account from the pool.
# 4-5 accounts = 4-5 users chat in parallel, zero waiting!
_pool = {}  # pool_idx -> Client
_pool_ready = set()  # pool_idx with connected clients
_pool_handlers = set()

# Keep single-account compat
userbot = None
userbot_ready = False


def pool_size():
    try:
        return len(get_redis().smembers("relay:pool"))
    except Exception:
        return 0


async def init_pool():
    """Start all pooled Telegram accounts."""
    global userbot, userbot_ready
    r = get_redis()
    try:
        idxs = sorted(r.smembers("relay:pool"), key=int)
    except Exception:
        idxs = []
    # Migrate legacy single session into pool slot 0
    if not idxs:
        legacy = r.get("relay:master_session") or os.getenv("SESSION_STRING", "")
        if legacy:
            r.sadd("relay:pool", "0")
            r.set("relay:pool:0", legacy)
            idxs = ["0"]
            logger.info("Migrated master session to pool slot 0")
    for idx in idxs:
        if idx in _pool_ready:
            continue
        ss = r.get(f"relay:pool:{idx}")
        if not ss:
            continue
        try:
            client = Client(f"pool_{idx}", api_id=config.API_ID,
                            api_hash=config.API_HASH, session_string=ss)
            await client.start()
            me = await client.get_me()
            logger.info(f"Pool[{idx}]: @{me.username} (id={me.id})")
            _pool[idx] = client
            _pool_ready.add(idx)
            if idx not in _pool_handlers:
                _pool_handlers.add(idx)
                bots = get_bots()
                names = [b.strip("@").lower() for b in bots]

                def make_handlers(pi):
                    @client.on_message(filters.chat(names) if names else filters.private)
                    async def on_msg(c, m: Message):
                        await route_pool_message(pi, m)

                    @client.on_edited_message(filters.chat(names) if names else filters.private)
                    async def on_edit(c, m: Message):
                        await route_pool_message(pi, m, edited=True)
                make_handlers(idx)
        except Exception as e:
            logger.error(f"Pool[{idx}] failed: {e}")
    # Compat: userbot = first pool client
    if _pool_ready:
        first = sorted(_pool_ready, key=int)[0]
        userbot = _pool[first]
        userbot_ready = True
    else:
        logger.info("Pool empty — relay disabled")


async def get_pool_client(web_session: str):
    """Get (or assign) a pool account for a web user. Sticky assignment."""
    r = get_redis()
    if not _pool_ready:
        await init_pool()
    if not _pool_ready:
        return None, None
    # Existing assignment?
    idx = r.get(f"relay:pool:assign:{web_session}")
    if idx and idx in _pool_ready:
        r.expire(f"relay:pool:assign:{web_session}", 1800)  # 30 min sticky
        return _pool[idx], idx
    # Find a free account
    assigned = set()
    for k in r.scan_iter("relay:pool:assign:*"):
        a = r.get(k)
        if a:
            assigned.add(a)
    free = [i for i in sorted(_pool_ready, key=int) if i not in assigned]
    if free:
        idx = free[0]
    else:
        # All busy — share the least-recently-used (fallback to pool[0])
        idx = sorted(_pool_ready, key=int)[0]
    r.setex(f"relay:pool:assign:{web_session}", 1800, idx)
    # Map pool account -> web session for routing replies
    r.setex(f"relay:pool:owner:{idx}", 1800, web_session)
    logger.info(f"[Pool] {web_session[:8]} -> account[{idx}]")
    return _pool[idx], idx


async def route_pool_message(pool_idx: str, message: Message, edited: bool = False):
    """Route a reply from a pool account to its assigned web user."""
    r = get_redis()
    web_session = r.get(f"relay:pool:owner:{pool_idx}")
    if not web_session:
        # No owner — try pending queue fallback
        bkey = bot_key(message.chat.username or str(message.chat.id))
        web_session = r.lpop(f"route:pending:{bkey}") or "_orphan"
    # Temporarily set lock so route_bot_message delivers to the right user
    bkey = bot_key(message.chat.username or str(message.chat.id))
    if not edited:
        # Mark as awaiting with timestamp (primary) + legacy pending queue
        if web_session and web_session != "_orphan":
            r.setex(f"route:awaiting:{bkey}:{web_session}", 300, str(time.time()))
        r.lpush(f"route:pending:{bkey}", web_session)
    # Refresh owner TTL on activity
    r.expire(f"relay:pool:owner:{pool_idx}", 1800)
    r.expire(f"relay:pool:assign:{web_session}", 1800)
    await route_bot_message(message, edited=edited)


# Legacy single-userbot init (kept for compat, delegates to pool)
async def init_userbot():
    await init_pool()


async def route_bot_message(message: Message, edited: bool = False):
    """Route a bot message/edit to the correct web user."""
    r = get_redis()
    bkey = bot_key(message.chat.username or str(message.chat.id))
    mid = message.id
    text = message.text or message.caption or "[media]"

    # --- extract buttons ---
    buttons = []
    if message.reply_markup and hasattr(message.reply_markup, "inline_keyboard"):
        for row in message.reply_markup.inline_keyboard:
            brow = []
            for btn in row:
                b = {"text": btn.text}
                if getattr(btn, "callback_data", None):
                    # cb_id assigned per-session later; store template
                    b["cb_data"] = btn.callback_data
                    brow.append(b)
                elif getattr(btn, "url", None):
                    b["url"] = btn.url
                    brow.append(b)
            if brow:
                buttons.append(brow)
    # NOTE: callback buttons need per-session cb_id; resolved at delivery

    if edited:
        # Find which session history contains this message
        for skey in r.scan_iter(f"chat:history:*:{bkey}"):
            msgs = r.lrange(skey, 0, -1)
            for i, mj in enumerate(msgs):
                m = json.loads(mj)
                if m.get("msg_id") == mid and m.get("from") == "bot":
                    m["text"] = text
                    m["edited"] = True
                    m["ts"] = time.time()  # bump ts so poll picks up the edit
                    # refresh buttons for this session
                    m["buttons"] = await make_buttons(r, skey.split(":")[2], bkey, mid, message)
                    r.lset(skey, i, json.dumps(m))
                    logger.info(f"[Route] Edit -> {skey}")
                    return
        # Edit target not found in any history (e.g., original went to orphan).
        # Fall through and deliver as a new message so the user still sees it.
        logger.info(f"[Route] Edit target {mid} not found, delivering as new")
        is_edit_fallback = True
    else:
        is_edit_fallback = False

    # --- dedup ---
    # For edit-fallbacks, use a separate key so the original's seen-flag doesn't block
    dedup_key = f"route:seen:{bkey}:{mid}:editfb" if (edited and is_edit_fallback) else f"route:seen:{bkey}:{mid}"
    if r.exists(dedup_key):
        return
    r.setex(dedup_key, 86400, "1")

    # --- find target session: most-recently-active awaiting user ---
    # Check lock holder first (active conversation)
    LOCK_TTL = 120
    target_sid = r.get(f"lock:{bkey}")
    if target_sid:
        r.expire(f"lock:{bkey}", LOCK_TTL)
    else:
        # Find the session with the most recent awaiting-response timestamp
        # This handles multi-user: the user who most recently sent/clicked gets the reply
        best_sid = None
        best_ts = 0
        for akey in r.scan_iter(f"route:awaiting:{bkey}:*"):
            try:
                ts = float(r.get(akey) or 0)
                if ts > best_ts:
                    best_ts = ts
                    # akey format: route:awaiting:{bkey}:{session_id}
                    best_sid = akey.split(":", 3)[3]
            except:
                pass
        if best_sid:
            target_sid = best_sid
            r.delete(f"route:awaiting:{bkey}:{best_sid}")
        else:
            # Fallback to legacy pending queue
            target_sid = r.lpop(f"route:pending:{bkey}")
    if not target_sid:
        logger.info(f"[Route] No pending user for @{bkey}, storing in orphan")
        target_sid = "_orphan"

    await deliver_to_session(r, target_sid, bkey, mid, text, buttons, message)


async def make_buttons(r, session_id, bkey, mid, message):
    """Build per-session clickable buttons."""
    out = []
    if message.reply_markup and hasattr(message.reply_markup, "inline_keyboard"):
        for row in message.reply_markup.inline_keyboard:
            brow = []
            for btn in row:
                b = {"text": btn.text}
                if getattr(btn, "callback_data", None):
                    cb_id = f"{session_id}:{bkey}:{mid}:{btn.callback_data[:40]}"
                    b["cb_id"] = cb_id
                    r.setex(f"cb:{cb_id}", 3600, json.dumps({
                        "session": session_id, "chat_id": message.chat.id,
                        "msg_id": mid, "data": btn.callback_data, "bkey": bkey,
                    }))
                    brow.append(b)
                elif getattr(btn, "url", None):
                    b["url"] = btn.url
                    brow.append(b)
            if brow:
                out.append(brow)
    return out


async def deliver_to_session(r, session_id, bkey, mid, text, buttons_tpl, message):
    """Store a bot message in a web user's history."""
    hkey = f"chat:history:{session_id}:{bkey}"
    buttons = await make_buttons(r, session_id, bkey, mid, message)
    msg_data = json.dumps({
        "from": "bot", "text": text, "ts": time.time(),
        "buttons": buttons, "msg_id": mid,
    })
    r.rpush(hkey, msg_data)
    r.ltrim(hkey, -200, -1)
    logger.info(f"[Route] Msg @{bkey} -> {session_id[:8] if len(session_id)>8 else session_id}")


# ------------------------------------------------------------ Admin auth ----
_pending = {}


@web.post("/api/admin/phone")
async def admin_phone(phone: str = Form(...)):
    """Admin: add a Telegram account to the pool (step 1: send OTP)."""
    try:
        tmp = Client("admin_auth", api_id=config.API_ID, api_hash=config.API_HASH, workdir="/tmp")
        await tmp.connect()
        sent = await tmp.send_code(phone.strip())
        _pending["admin"] = {"client": tmp, "phone": phone.strip(), "hash": sent.phone_code_hash}
        logger.info(f"Admin OTP sent to {phone.strip()}")
        return RedirectResponse(url="/?otp=sent", status_code=303)
    except Exception as e:
        return RedirectResponse(url="/?err=" + str(e)[:100], status_code=303)


@web.post("/api/admin/verify")
async def admin_verify(code: str = Form(...)):
    """Admin: verify OTP and ADD this account to the pool."""
    p = _pending.get("admin")
    if not p:
        return RedirectResponse(url="/?err=no_auth", status_code=303)
    try:
        tmp = p["client"]
        await tmp.sign_in(p["phone"], p["hash"], code.strip())
        ss = await tmp.export_session_string()
        me = await tmp.get_me()
        await tmp.disconnect()
        _pending.pop("admin", None)
        r = get_redis()
        # Find next free pool slot
        idx = 0
        existing = r.smembers("relay:pool")
        while str(idx) in existing:
            idx += 1
        r.sadd("relay:pool", str(idx))
        r.set(f"relay:pool:{idx}", ss)
        r.set(f"relay:pool:{idx}:phone", p["phone"])
        r.set(f"relay:pool:{idx}:name", f"@{me.username}" if me.username else str(me.id))
        logger.info(f"Pool account [{idx}] added: {p['phone']}")
        await init_pool()
        return RedirectResponse(url=f"/?ok=account_added_{idx}", status_code=303)
    except Exception as e:
        return RedirectResponse(url="/?err=" + str(e)[:100], status_code=303)


@web.post("/api/admin/pool/remove")
async def admin_pool_remove(idx: str = Form(...)):
    """Remove a pool account."""
    r = get_redis()
    r.srem("relay:pool", idx)
    r.delete(f"relay:pool:{idx}", f"relay:pool:{idx}:phone", f"relay:pool:{idx}:name")
    c = _pool.pop(idx, None)
    _pool_ready.discard(idx)
    _pool_handlers.discard(idx)
    if c:
        try:
            await c.stop()
        except Exception:
            pass
    return RedirectResponse(url="/", status_code=303)


@web.get("/api/admin/pool/list")
async def admin_pool_list():
    """List pool accounts."""
    r = get_redis()
    out = []
    for idx in sorted(r.smembers("relay:pool"), key=int):
        out.append({
            "idx": idx,
            "phone": r.get(f"relay:pool:{idx}:phone") or "?",
            "name": r.get(f"relay:pool:{idx}:name") or "?",
            "connected": idx in _pool_ready,
        })
    return JSONResponse({"ok": True, "pool": out, "size": len(out)})


# ------------------------------------------------------------ Chat API ------
@web.get("/api/chat/bots")
async def chat_bots():
    return JSONResponse({"ok": True, "bots": get_bots()})


@web.post("/api/chat/send")
async def chat_send(session_id: str = Form(...), bot: str = Form(...), text: str = Form(...)):
    target = norm_bot(bot)
    if target not in get_bots():
        return JSONResponse({"ok": False, "error": "Bot not available."}, status_code=400)
    # Get this user's dedicated pool account
    client, pool_idx = await get_pool_client(session_id)
    if not client:
        return JSONResponse({"ok": False, "error": "Relay offline — admin setup needed."}, status_code=503)
    try:
        r = get_redis()
        bkey = bot_key(target)
        hkey = f"chat:history:{session_id}:{bkey}"
        t = text[:500]
        # Auto /start so default/welcome messages show
        if r.llen(hkey) == 0 and not t.strip().startswith("/"):
            await client.send_message(target, "/start")
            r.rpush(hkey, json.dumps({"from": "user", "text": "/start", "ts": time.time()}))
            await asyncio.sleep(1)
        r.rpush(hkey, json.dumps({"from": "user", "text": t, "ts": time.time()}))
        r.ltrim(hkey, -200, -1)
        # Mark awaiting reply with timestamp (primary routing)
        # Also keep legacy pending queue as fallback
        r.setex(f"route:awaiting:{bkey}:{session_id}", 300, str(time.time()))
        r.rpush(f"route:pending:{bkey}", session_id)
        r.expire(f"route:pending:{bkey}", 300)
        await client.send_message(target, t)
        logger.info(f"[Send:{session_id[:8]} via pool[{pool_idx}]] -> {target}: {t[:40]}")
        return JSONResponse({"ok": True, "pool": pool_idx})
    except Exception as e:
        logger.error(f"Send failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


async def queue_processor():
    """Background task: when a bot's lock expires, send the next queued message."""
    LOCK_TTL = 120
    while True:
        try:
            await asyncio.sleep(10)
            if not userbot_ready or not userbot:
                continue
            r = get_redis()
            for b in get_bots():
                bkey = bot_key(b)
                if r.get(f"lock:{bkey}"):
                    continue  # still locked
                qitem = r.lpop(f"queue:{bkey}")
                if not qitem:
                    continue
                q = json.loads(qitem)
                sid, t = q["session"], q["text"]
                hkey = f"chat:history:{sid}:{bkey}"
                try:
                    r.setex(f"lock:{bkey}", LOCK_TTL, sid)
                    await userbot.send_message(norm_bot(b), t)
                    r.rpush(f"route:pending:{bkey}", sid)
                    # Mark as sent (update the queued marker)
                    logger.info(f"[Queue] Sent queued msg for {sid[:8]} -> @{bkey}")
                except Exception as e:
                    logger.error(f"[Queue] Failed to send for {sid[:8]}: {e}")
                    r.delete(f"lock:{bkey}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Queue processor error: {e}")
            await asyncio.sleep(10)


@web.get("/api/chat/messages")
async def chat_messages(session_id: str = Query(""), bot: str = Query(""), since: float = Query(0)):
    try:
        r = get_redis()
        hkey = f"chat:history:{session_id}:{bot_key(bot)}"
        msgs = [json.loads(m) for m in r.lrange(hkey, 0, -1)]
        return JSONResponse({"ok": True, "messages": [m for m in msgs if m.get("ts", 0) > since]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.post("/api/chat/callback")
async def chat_callback(session_id: str = Form(...), cb_id: str = Form(...)):
    client, pool_idx = await get_pool_client(session_id)
    if not client:
        return JSONResponse({"ok": False, "error": "Relay offline."}, status_code=503)
    try:
        r = get_redis()
        cj = r.get(f"cb:{cb_id}")
        if not cj:
            return JSONResponse({"ok": False, "error": "Button expired."}, status_code=400)
        cb = json.loads(cj)
        if cb.get("session") != session_id:
            return JSONResponse({"ok": False, "error": "Not yours."}, status_code=403)
        await client.request_callback_answer(
            chat_id=cb["chat_id"], message_id=cb["msg_id"], callback_data=cb["data"])
        bkey = cb.get("bkey", "")
        if bkey:
            r.setex(f"route:awaiting:{bkey}:{session_id}", 300, str(time.time()))
            r.rpush(f"route:pending:{bkey}", session_id)
        logger.info(f"[Click:{session_id[:8]} via pool[{pool_idx}]] {cb['data'][:40]}")
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.get("/api/chat/commands")
async def chat_commands(bot: str = Query("")):
    bkey = bot_key(bot)
    try:
        custom = get_redis().get(f"relay:cmds_custom:{bkey}")
        if custom:
            return JSONResponse({"ok": True, "commands": json.loads(custom), "source": "custom"})
    except Exception:
        pass
    if not _pool_ready:
        return JSONResponse({"ok": True, "commands": []})
    try:
        from pyrogram import raw
        client = _pool[sorted(_pool_ready, key=int)[0]]
        target = norm_bot(bot)
        peer = await client.resolve_peer(target)
        result = await client.invoke(raw.functions.bots.GetBotCommands(bot=peer))
        cmds = [{"command": c.command, "description": c.description} for c in result]
        if cmds:
            get_redis().setex(f"relay:cmds:{bkey}", 3600, json.dumps(cmds))
            return JSONResponse({"ok": True, "commands": cmds, "source": "auto"})
    except Exception as e:
        logger.warning(f"GetBotCommands failed: {e}")
    try:
        cached = get_redis().get(f"relay:cmds:{bkey}")
        if cached:
            return JSONResponse({"ok": True, "commands": json.loads(cached), "cached": True})
    except Exception:
        pass
    return JSONResponse({"ok": True, "commands": []})


@web.post("/api/admin/bots/discover")
async def admin_discover_commands(bot_username: str = Form(...)):
    """Auto-discover a bot's real commands by sending /help and /start, parsing responses."""
    if not _pool_ready:
        return JSONResponse({"ok": False, "error": "Relay offline."}, status_code=503)
    import re
    client = _pool[sorted(_pool_ready, key=int)[0]]
    target = norm_bot(bot_username)
    bkey = bot_key(target)
    # Use a dedicated discovery session so routing doesn't mix with users
    disc_sid = "_discover"
    r = get_redis()
    # Clear old discovery history
    r.delete(f"chat:history:{disc_sid}:{bkey}")
    found = {}
    try:
        # Send /start and /help, collect responses
        for cmd in ["/start", "/help"]:
            r.rpush(f"route:pending:{bkey}", disc_sid)
            await client.send_message(target, cmd)
            await asyncio.sleep(3)  # wait for reply
        # Read what came back
        msgs = r.lrange(f"chat:history:{disc_sid}:{bkey}", 0, -1)
        for mj in msgs:
            m = json.loads(mj)
            if m.get("from") != "bot":
                continue
            text = m.get("text", "")
            # Find /commands in text
            for cm in re.finditer(r"/(\w+)", text):
                c = cm.group(1).lower()
                if c not in found and c not in ("start",):
                    found[c] = ""
            # Also check button texts that look like commands
            for row in m.get("buttons", []):
                for b in row:
                    bt = b.get("text", "")
                    cm = re.match(r"^/(\w+)", bt)
                    if cm and cm.group(1).lower() not in found:
                        found[cm.group(1).lower()] = bt
        # Always include /start
        cmds = [{"command": "start", "description": "Start"}]
        for c, d in sorted(found.items()):
            cmds.append({"command": c, "description": d})
        if len(cmds) > 1:
            r.set(f"relay:cmds_custom:{bkey}", json.dumps(cmds))
            logger.info(f"Discovered {len(cmds)} commands for @{bkey}: {[c['command'] for c in cmds]}")
        # Cleanup
        r.delete(f"chat:history:{disc_sid}:{bkey}")
        return JSONResponse({"ok": True, "commands": cmds, "source": "discovered"})
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.post("/api/admin/bots/commands")
async def admin_set_commands(bot_username: str = Form(...), commands: str = Form(...)):
    """Admin: manually set custom commands for a bot (one per line: /cmd description)."""
    bkey = bot_key(bot_username)
    cmds = []
    for line in commands.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        cmd = parts[0].lstrip("/")
        desc = parts[1] if len(parts) > 1 else ""
        cmds.append({"command": cmd, "description": desc})
    get_redis().set(f"relay:cmds_custom:{bkey}", json.dumps(cmds[:10]))  # max 10
    logger.info(f"Custom commands set for @{bkey}: {len(cmds)}")
    return RedirectResponse(url="/", status_code=303)


@web.get("/api/admin/bots/commands/list")
async def admin_list_commands(bot: str = Query("")):
    """List current custom commands for a bot."""
    try:
        custom = get_redis().get(f"relay:cmds_custom:{bot_key(bot)}")
        cmds = json.loads(custom) if custom else []
        return JSONResponse({"ok": True, "commands": cmds})
    except Exception:
        return JSONResponse({"ok": True, "commands": []})


@web.post("/api/admin/bots/commands/add")
async def admin_add_command(bot_username: str = Form(...), command: str = Form(...), description: str = Form("")):
    """Add a single command to a bot's list (max 10)."""
    bkey = bot_key(bot_username)
    r = get_redis()
    try:
        custom = r.get(f"relay:cmds_custom:{bkey}")
        cmds = json.loads(custom) if custom else []
    except Exception:
        cmds = []
    cmd = command.strip().lstrip("/")
    if cmd and not any(c["command"] == cmd for c in cmds) and len(cmds) < 10:
        cmds.append({"command": cmd, "description": description.strip()})
        r.set(f"relay:cmds_custom:{bkey}", json.dumps(cmds))
    return JSONResponse({"ok": True, "commands": cmds})


@web.post("/api/admin/bots/commands/delete")
async def admin_delete_command(bot_username: str = Form(...), command: str = Form(...)):
    """Delete a single command from a bot's list."""
    bkey = bot_key(bot_username)
    r = get_redis()
    try:
        custom = r.get(f"relay:cmds_custom:{bkey}")
        cmds = json.loads(custom) if custom else []
    except Exception:
        cmds = []
    cmd = command.strip().lstrip("/")
    cmds = [c for c in cmds if c["command"] != cmd]
    r.set(f"relay:cmds_custom:{bkey}", json.dumps(cmds))
    return JSONResponse({"ok": True, "commands": cmds})


@web.post("/api/admin/bots/commands/clear")
async def admin_clear_commands(bot_username: str = Form(...)):
    """Clear all custom commands for a bot."""
    get_redis().delete(f"relay:cmds_custom:{bot_key(bot_username)}")
    return JSONResponse({"ok": True, "commands": []})


@web.post("/api/admin/bots/add")
async def admin_add_bot(bot_username: str = Form(...)):
    b = norm_bot(bot_username)
    if b:
        get_redis().sadd("relay:bots", b)
    return RedirectResponse(url="/", status_code=303)


@web.post("/api/admin/bots/remove")
async def admin_remove_bot(bot_username: str = Form(...)):
    get_redis().srem("relay:bots", norm_bot(bot_username))
    return RedirectResponse(url="/", status_code=303)


# ------------------------------------------------------------ Dashboard -----
@web.get("/", response_class=HTMLResponse)
async def dashboard(ok: str = "", otp: str = "", err: str = ""):
    banner = ""
    if ok.startswith("account_added"):
        banner = '<div style="background:#1a4d1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d7a2d;">✅ Telegram account added to pool!</div>'
    elif ok == "connected":
        banner = '<div style="background:#1a4d1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d7a2d;">✅ Connected!</div>'
    elif otp == "sent":
        banner = '<div style="background:#1a3a4d;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d6a7a;">📩 OTP sent! Enter the code below.</div>'
    elif err:
        banner = f'<div style="background:#4d1a1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #7a2d2d;">❌ {err}</div>'
    relay = f"🟢 {len(_pool_ready)} accounts" if _pool_ready else "🔴 Not connected"
    bots = get_bots()
    bots_html = "".join(
        f'<li>{b} <form action="/api/admin/bots/remove" method="post" style="display:inline">'
        f'<input type="hidden" name="bot_username" value="{b}">'
        f'<button type="submit" style="width:auto;padding:4px 10px;">✕</button></form></li>'
        for b in bots)
    return f"""
    <!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Pool Relay Dashboard</title>
    <style>
        body {{ font-family: Arial; background: #0f0f0f; color: #fff; padding: 20px; max-width: 500px; margin: auto; }}
        h1 {{ color: #d4af37; }} .card {{ background: #1a1a1a; padding: 15px; border-radius: 8px; margin: 10px 0; }}
        input, button, select {{ width: 100%; padding: 12px; margin: 8px 0; border-radius: 8px; border: 1px solid #333; font-size: 16px; box-sizing: border-box; }}
        input, select {{ background: #222; color: #fff; }} button {{ background: #d4af37; color: #000; font-weight: bold; border: none; cursor: pointer; }}
        a {{ color: #d4af37; }} ul {{ padding-left: 20px; }} li {{ margin: 6px 0; }}
    </style></head>
    <body>
        <h1>🤖 Account Pool Relay</h1>{banner}
        <div class="card"><b>Relay:</b> {relay}<div id="poollist"></div></div>
        <div class="card"><h3>📱 Add Telegram Account (4-5 টা দাও!)</h3>
            <form action="/api/admin/phone" method="post">
                <input name="phone" placeholder="+8801XXXXXXXXX" required>
                <button type="submit">Send OTP</button></form>
            <form action="/api/admin/verify" method="post">
                <input name="code" placeholder="OTP Code" required>
                <button type="submit">Verify & Add to Pool</button></form></div>
        <div class="card"><h3>🤖 Bots</h3><ul>{bots_html}</ul>
            <form action="/api/admin/bots/add" method="post">
                <input name="bot_username" placeholder="@BotUsername" required>
                <button type="submit">➕ Add Bot</button></form></div>
        <div class="card"><h3>📋 Bot Commands Manager</h3>
            <p style="font-size:13px;color:#888;">Bot select করো, commands দেখো, add/delete করো (সর্বোচ্চ ১০টি)!</p>
            <select id="cmdbot">
                <option value="">-- Bot select করো --</option>
            </select>
            <div id="cmdlist" style="margin:10px 0;"></div>
            <div style="display:flex;gap:6px;">
                <input id="newcmd" placeholder="/command" style="flex:1;">
                <input id="newdesc" placeholder="বিবরণ" style="flex:2;">
            </div>
            <button onclick="addCmd()" style="margin-top:6px;">➕ Add Command</button>
            <button onclick="discoverCmd()" style="background:#2a4d2a;margin-top:6px;">🔍 Auto-Discover</button>
            <p id="cmdmsg" style="font-size:13px;color:#d4af37;"></p>
        </div>
        <div class="card"><a href="/chat">💬 Open Live Chat →</a></div>
        <div class="card"><a href="/api/status">📊 Status</a></div>
        <script>
        fetch('/api/admin/pool/list').then(r=>r.json()).then(d=>{{
            const div = document.getElementById('poollist');
            div.innerHTML = '<ul>' + (d.pool||[]).map(p=>
                '<li>['+p.idx+'] '+p.phone+' ('+p.name+') '+(p.connected?'🟢':'🔴')+
                ' <form action="/api/admin/pool/remove" method="post" style="display:inline">'+
                '<input type="hidden" name="idx" value="'+p.idx+'">'+
                '<button type="submit" style="width:auto;padding:4px 10px;">✕</button></form></li>'
            ).join('') + '</ul>';
        }});
        async function loadCmdBots() {{
            const d = await fetch('/api/chat/bots').then(r=>r.json());
            const sel = document.getElementById('cmdbot');
            sel.innerHTML = '<option value="">-- Bot select করো --</option>';
            (d.bots||[]).forEach(b=>{{
                const o = document.createElement('option'); o.value=b; o.textContent=b; sel.appendChild(o);
            }});
        }}
        async function loadCmdList() {{
            const bot = document.getElementById('cmdbot').value;
            const div = document.getElementById('cmdlist');
            if (!bot) {{ div.innerHTML=''; return; }}
            const d = await fetch('/api/admin/bots/commands/list?bot='+encodeURIComponent(bot)).then(r=>r.json());
            div.innerHTML = '';
            (d.commands||[]).forEach((c,i)=>{{
                const row = document.createElement('div');
                row.style.cssText = 'display:flex;align-items:center;gap:8px;padding:8px;background:#222;border-radius:8px;margin:4px 0;';
                row.innerHTML = '<b style="color:#d4af37;">/'+c.command+'</b><span style="flex:1;color:#aaa;font-size:13px;">'+(c.description||'')+'</span>';
                const del = document.createElement('button');
                del.textContent = '✕'; del.style.cssText = 'width:auto;padding:6px 12px;background:#5a1a1a;';
                del.onclick = ()=>delCmd(bot, c.command);
                row.appendChild(del); div.appendChild(row);
            }});
            if (!(d.commands||[]).length) div.innerHTML = '<p style="color:#888;font-size:13px;">কোনো command নাই — নিচে add করো!</p>';
        }}
        async function addCmd() {{
            const bot = document.getElementById('cmdbot').value;
            const cmd = document.getElementById('newcmd').value.trim();
            const desc = document.getElementById('newdesc').value.trim();
            if (!bot || !cmd) {{ document.getElementById('cmdmsg').textContent = '❌ Bot + command দাও!'; return; }}
            const r = await fetch('/api/admin/bots/commands/add', {{method:'POST',
                headers:{{'Content-Type':'application/x-www-form-urlencoded'}},
                body:'bot_username='+encodeURIComponent(bot)+'&command='+encodeURIComponent(cmd)+'&description='+encodeURIComponent(desc)}}).then(x=>x.json());
            document.getElementById('newcmd').value=''; document.getElementById('newdesc').value='';
            document.getElementById('cmdmsg').textContent = '✅ Added! ('+r.commands.length+'/10)';
            loadCmdList();
        }}
        async function delCmd(bot, cmd) {{
            await fetch('/api/admin/bots/commands/delete', {{method:'POST',
                headers:{{'Content-Type':'application/x-www-form-urlencoded'}},
                body:'bot_username='+encodeURIComponent(bot)+'&command='+encodeURIComponent(cmd)}});
            loadCmdList();
        }}
        async function discoverCmd() {{
            const bot = document.getElementById('cmdbot').value;
            if (!bot) return;
            document.getElementById('cmdmsg').textContent = '🔍 Discovering...';
            const r = await fetch('/api/admin/bots/discover', {{method:'POST',
                headers:{{'Content-Type':'application/x-www-form-urlencoded'}},
                body:'bot_username='+encodeURIComponent(bot)}}).then(x=>x.json());
            document.getElementById('cmdmsg').textContent = r.ok ? '✅ Found '+r.commands.length+' commands!' : '❌ '+r.error;
            loadCmdList();
        }}
        document.getElementById('cmdbot').addEventListener('change', loadCmdList);
        loadCmdBots();
        </script>
    </body></html>
    """


@web.get("/chat", response_class=HTMLResponse)
async def chat_ui():
    return """
    <!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Live Chat</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: Arial; background: #0f0f0f; color: #fff; height: 100vh; display: flex; flex-direction: column; }
        #header { background: #1a1a1a; padding: 12px; text-align: center; border-bottom: 1px solid #333; }
        #header b { color: #d4af37; }
        #bottabs { display: flex; gap: 6px; padding: 10px; background: #141414; overflow-x: auto; border-bottom: 1px solid #333; }
        .btab { padding: 10px 16px; border-radius: 12px; border: 1px solid #444; background: #1a1a1a; color: #aaa; cursor: pointer; white-space: nowrap; font-size: 14px; font-weight: bold; }
        .btab.active { border-color: #d4af37; color: #d4af37; background: #2a2410; }
        .btab .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: #4a4a4a; margin-right: 6px; }
        .btab.active .dot { background: #4caf50; }
        #status { text-align: center; padding: 6px; font-size: 12px; color: #888; }
        #cmdbar { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px; background: #141414; border-bottom: 1px solid #333; }
        #messages { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 8px; }
        .msg { max-width: 85%; padding: 10px 14px; border-radius: 12px; word-wrap: break-word; white-space: pre-wrap; }
        .msg.user { align-self: flex-end; background: #d4af37; color: #000; }
        .msg.bot { align-self: flex-start; background: #222; border: 1px solid #333; }
        .btnrow { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
        .tbtn { padding: 8px 12px; border-radius: 8px; border: 1px solid #d4af37; background: transparent; color: #d4af37; cursor: pointer; font-size: 14px; }
        .tbtn:active { background: #d4af37; color: #000; }
        #inputbar { display: flex; padding: 10px; gap: 8px; background: #1a1a1a; border-top: 1px solid #333; }
        #inputbar input { flex: 1; padding: 12px; border-radius: 8px; border: 1px solid #333; background: #222; color: #fff; font-size: 16px; }
        #inputbar button { padding: 12px 20px; border-radius: 8px; border: none; background: #d4af37; color: #000; font-weight: bold; cursor: pointer; }
    </style></head>
    <body>
        <div id="header"><b>💬 Live Chat</b></div>
        <div id="bottabs"></div>
        <div id="status">Connecting...</div>
        <div id="cmdbar" style="display:none;"></div>
        <div id="messages"></div>
        <div id="inputbar">
            <input id="inp" placeholder="Type a message..." autocomplete="off">
            <button onclick="send()">Send</button>
        </div>
        <script>
            let sid = localStorage.getItem('tg_sid') || ('u' + Math.random().toString(36).slice(2) + Date.now().toString(36));
            localStorage.setItem('tg_sid', sid);
            let curBot = '', lastTs = 0;
            const box = document.getElementById('messages');
            async function loadBots() {
                const d = await fetch('/api/chat/bots').then(x=>x.json());
                const tabs = document.getElementById('bottabs');
                tabs.innerHTML = '';
                (d.bots||[]).forEach((b,i) => {
                    const t = document.createElement('button');
                    t.className = 'btab' + (i===0?' active':'');
                    t.innerHTML = '<span class="dot"></span>' + b;
                    t.onclick = () => switchBot(b);
                    t.dataset.bot = b;
                    tabs.appendChild(t);
                });
                if (d.bots && d.bots.length) switchBot(d.bots[0]);
            }
            function switchBot(b) {
                curBot = b; lastTs = 0; box.innerHTML = '';
                document.querySelectorAll('.btab').forEach(t=>t.classList.toggle('active', t.dataset.bot===b));
                loadCommands(); poll();
            }
            async function loadCommands() {
                const bar = document.getElementById('cmdbar');
                try {
                    const d = await fetch('/api/chat/commands?bot='+encodeURIComponent(curBot)).then(x=>x.json());
                    if (d.ok && d.commands && d.commands.length) {
                        bar.style.display='flex'; bar.innerHTML='';
                        d.commands.forEach(c => {
                            const btn = document.createElement('button');
                            btn.className='tbtn'; btn.textContent='/'+c.command; btn.title=c.description||'';
                            btn.onclick=()=>{document.getElementById('inp').value='/'+c.command; send();};
                            bar.appendChild(btn);
                        });
                    } else bar.style.display='none';
                } catch(e){ bar.style.display='none'; }
            }
            async function poll() {
                if (!curBot) return;
                try {
                    const d = await fetch('/api/chat/messages?session_id='+sid+'&bot='+encodeURIComponent(curBot)+'&since='+lastTs).then(x=>x.json());
                    if (d.ok && d.messages.length) {
                        d.messages.forEach(m => {
                            lastTs = Math.max(lastTs, m.ts);
                            const mid = 'm' + (m.msg_id || m.ts);
                            let div = document.getElementById(mid);
                            const isNew = !div;
                            if (isNew) {
                                div = document.createElement('div');
                                div.id = mid;
                            }
                            div.className = 'msg ' + m.from;
                            div.textContent = m.text + (m.edited ? ' (edited)' : '');
                            if (m.buttons && m.buttons.length) {
                                m.buttons.forEach(row => {
                                    const rd = document.createElement('div'); rd.className='btnrow';
                                    row.forEach(b => {
                                        const btn = document.createElement('button');
                                        btn.className='tbtn'; btn.textContent=b.text;
                                        if (b.url) btn.onclick=()=>window.open(b.url,'_blank');
                                        else if (b.cb_id) btn.onclick=()=>clickBtn(b.cb_id,btn);
                                        rd.appendChild(btn);
                                    });
                                    div.appendChild(rd);
                                });
                            }
                            if (isNew) box.appendChild(div);
                        });
                        box.scrollTop = box.scrollHeight;
                    }
                    document.getElementById('status').textContent = '🟢 ' + curBot;
                } catch(e) {}
            }
            async function send() {
                const inp = document.getElementById('inp');
                const t = inp.value.trim();
                if (!t || !curBot) return;
                inp.value = '';
                const r = await fetch('/api/chat/send', {method:'POST',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:'session_id='+sid+'&bot='+encodeURIComponent(curBot)+'&text='+encodeURIComponent(t)}).then(x=>x.json());
                if (r.queued) {
                    document.getElementById('status').textContent = '⏳ ' + r.message;
                }
                setTimeout(poll, 800);
            }
            async function clickBtn(cb_id, btn) {
                btn.disabled = true;
                await fetch('/api/chat/callback', {method:'POST',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:'session_id='+sid+'&cb_id='+encodeURIComponent(cb_id)});
                setTimeout(poll, 1500);
            }
            document.getElementById('inp').addEventListener('keypress', e=>{if(e.key==='Enter')send();});
            setInterval(()=>{if(curBot)poll();}, 2000);
            loadBots();
        </script>
    </body></html>
    """


@web.get("/api/status")
async def system_status():
    redis_ok = "unknown"
    try:
        r = get_redis(); r.ping(); redis_ok = "connected"
    except Exception as e:
        redis_ok = f"error: {e}"
    return {
        "status": "running", "bot": config.BOT_USERNAME, "redis": redis_ok,
        "pool_size": len(_pool_ready), "pool_total": pool_size(),
        "bots": get_bots(),
    }


@web.get("/health")
async def health():
    return {"ok": True}


# --------------------------------------------------------------- @mlazone_bot
import aiohttp
_bot_api_token = config.BOT_TOKEN
_poller_offset = 0

async def bot_api_send(chat_id: int, text: str):
    async with aiohttp.ClientSession() as s:
        async with s.post(
            f"https://api.telegram.org/bot{_bot_api_token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        ) as r:
            return await r.json()

async def bot_api_poller():
    global _poller_offset
    if not _bot_api_token:
        return
    logger.info("Starting Bot API poller...")
    async with aiohttp.ClientSession() as s:
        while True:
            try:
                async with s.get(
                    f"https://api.telegram.org/bot{_bot_api_token}/getUpdates",
                    params={"offset": _poller_offset, "timeout": 30},
                ) as r:
                    data = await r.json()
                for upd in data.get("result", []):
                    _poller_offset = upd["update_id"] + 1
                    msg = upd.get("message", {})
                    text = msg.get("text", "")
                    chat_id = msg.get("chat", {}).get("id")
                    if not text or not chat_id:
                        continue
                    if text.startswith("/start"):
                        await bot_api_send(chat_id, "👋 Hello! I'm @mlazone_bot\n\n💬 Live chat: https://telegram-userbot-production-d621.up.railway.app/chat")
                    else:
                        await bot_api_send(chat_id, f"Received: {text[:100]}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Poller error: {e}")
                await asyncio.sleep(5)


bot = Client(config.SESSION_NAME, api_id=config.API_ID,
             api_hash=config.API_HASH, bot_token=config.BOT_TOKEN or None)

@bot.on_message(filters.private & filters.text)
async def handle_dm(client: Client, message: Message):
    await message.reply_text(f"Received: {message.text[:100]}")


# ----------------------------------------------------------------- Runner ---
async def main():
    logger.info("Starting master relay...")
    try:
        r = get_redis()
        for b in DEFAULT_BOTS:
            r.sadd("relay:bots", b)
    except Exception:
        pass
    await bot.start()
    me = await bot.get_me()
    logger.info(f"Bot @{me.username} online")
    asyncio.create_task(bot_api_poller())
    asyncio.create_task(queue_processor())
    await init_userbot()
    logger.info("Starting web server...")
    server = uvicorn.Server(uvicorn.Config(web, host=config.WEB_HOST, port=config.WEB_PORT, log_level="info"))
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
