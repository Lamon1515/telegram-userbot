"""
Multi-bot, multi-user live chat relay.

Architecture:
- Each web user logs in with their OWN Telegram phone -> personal userbot session
- Each user can chat with MULTIPLE bots (admin-configured list)
- Per-user, per-bot isolated chat histories
- Bot commands auto-fetched, inline buttons supported, edit handling, dedup

Endpoints:
- GET  /                    - Dashboard (admin: bots, status)
- GET  /chat                - Live chat UI (embeddable, per-user login)
- POST /api/user/phone      - User step 1: send OTP to their phone
- POST /api/user/verify     - User step 2: verify OTP, save personal session
- GET  /api/user/status     - Check if this web user has a Telegram session
- POST /api/user/logout     - Clear this user's session
- GET  /api/chat/bots       - List available bots
- POST /api/chat/send       - Send message to a bot (per-user)
- GET  /api/chat/messages   - Get messages (per-user, per-bot)
- POST /api/chat/callback   - Click inline button (per-user)
- GET  /api/chat/commands   - Get bot's commands (per-user)
- POST /api/admin/bots/add    - Admin: add a bot
- POST /api/admin/bots/remove - Admin: remove a bot
- GET  /api/status - System status
- GET  /health     - Health check
"""
import asyncio
import json
import logging
import os
import time
import uuid

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

web = FastAPI(title="Lamon Multi-Bot Live Chat")


def get_redis():
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD or None,
        db=config.REDIS_DB,
        decode_responses=True,
    )


def get_bots():
    """Get the list of available bots (admin-configured)."""
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


# ------------------------------------------------------------ Userbots ------
# Personal userbot clients, keyed by web session_id
_userbots = {}  # session_id -> Client
_userbots_ready = set()  # session_ids with connected clients
_handlers_registered = set()  # session_ids with handlers attached


async def get_userbot(session_id: str):
    """Get or create the personal userbot for a web session."""
    if not session_id:
        return None
    if session_id in _userbots_ready:
        return _userbots.get(session_id)
    # Try to load session from Redis
    try:
        r = get_redis()
        ss = r.get(f"relay:session:{session_id}")
        if not ss:
            return None
        client = Client(
            f"user_{session_id[:8]}",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=ss,
            workdir="/tmp",
        )
        await client.start()
        me = await client.get_me()
        logger.info(f"Personal userbot for {session_id[:8]}: @{me.username}")
        _userbots[session_id] = client
        _userbots_ready.add(session_id)
        # Register handlers once per client
        if session_id not in _handlers_registered:
            _handlers_registered.add(session_id)
            bots = get_bots()
            bot_usernames = [b.strip("@").lower() for b in bots]

            @client.on_message(filters.chat(bot_usernames) if bot_usernames else filters.private)
            async def handle_bot_msg(c, message: Message):
                await store_bot_message(session_id, message)

            @client.on_edited_message(filters.chat(bot_usernames) if bot_usernames else filters.private)
            async def handle_bot_edit(c, message: Message):
                await store_bot_message(session_id, message, edited=True)
        return client
    except Exception as e:
        logger.error(f"Userbot load failed for {session_id[:8]}: {e}")
        _userbots.pop(session_id, None)
        _userbots_ready.discard(session_id)
        return None


def bot_key(bot_username: str):
    return norm_bot(bot_username).strip("@").lower()


async def store_bot_message(session_id: str, message: Message, edited: bool = False):
    """Store a bot message for a specific user's chat (with dedup + buttons)."""
    bkey = bot_key(message.chat.username or str(message.chat.id))
    hkey = f"chat:history:{session_id}:{bkey}"
    mid = message.id
    r = get_redis()
    # Dedup via Redis (survives restarts)
    dedup_key = f"chat:seen:{session_id}:{bkey}:{mid}"
    if not edited and r.exists(dedup_key):
        return
    r.setex(dedup_key, 86400, "1")
    text = message.text or message.caption or "[media]"
    logger.info(f"[Relay:{session_id[:8]}] {'Edit' if edited else 'Msg'} from @{bkey}: {text[:60]}")
    buttons = []
    if message.reply_markup and hasattr(message.reply_markup, "inline_keyboard"):
        for row in message.reply_markup.inline_keyboard:
            brow = []
            for btn in row:
                b = {"text": btn.text}
                if getattr(btn, "callback_data", None):
                    cb_id = f"{session_id}:{bkey}:{mid}:{btn.callback_data[:40]}"
                    brow.append({**b, "cb_id": cb_id})
                    r.setex(f"cb:{cb_id}", 3600, json.dumps({
                        "session": session_id,
                        "chat_id": message.chat.id,
                        "msg_id": mid,
                        "data": btn.callback_data,
                    }))
                elif getattr(btn, "url", None):
                    b["url"] = btn.url
                    brow.append(b)
            if brow:
                buttons.append(brow)
    if edited:
        all_msgs = r.lrange(hkey, 0, -1)
        for i, mjson in enumerate(all_msgs):
            m = json.loads(mjson)
            if m.get("msg_id") == mid and m.get("from") == "bot":
                m["text"] = text
                m["buttons"] = buttons
                m["edited"] = True
                r.lset(hkey, i, json.dumps(m))
                break
    else:
        msg_data = json.dumps({
            "from": "bot", "text": text, "ts": time.time(),
            "buttons": buttons, "msg_id": mid,
        })
        r.rpush(hkey, msg_data)
        r.ltrim(hkey, -200, -1)


# ------------------------------------------------------------ User auth -----
_pending = {}  # web_session -> {client, phone, hash}


@web.post("/api/user/phone")
async def user_phone(session_id: str = Form(...), phone: str = Form(...)):
    """User step 1: send OTP to their own phone."""
    try:
        tmp = Client(f"auth_{session_id[:8]}", api_id=config.API_ID,
                      api_hash=config.API_HASH, workdir="/tmp")
        await tmp.connect()
        sent = await tmp.send_code(phone.strip())
        _pending[session_id] = {"client": tmp, "phone": phone.strip(),
                                "hash": sent.phone_code_hash}
        logger.info(f"OTP sent to {phone.strip()} for {session_id[:8]}")
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Send code failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.post("/api/user/verify")
async def user_verify(session_id: str = Form(...), code: str = Form(...)):
    """User step 2: verify OTP, save their personal session."""
    p = _pending.get(session_id)
    if not p:
        return JSONResponse({"ok": False, "error": "Send phone first."}, status_code=400)
    try:
        tmp = p["client"]
        await tmp.sign_in(p["phone"], p["hash"], code.strip())
        ss = await tmp.export_session_string()
        await tmp.disconnect()
        _pending.pop(session_id, None)
        get_redis().set(f"relay:session:{session_id}", ss)
        logger.info(f"Session saved for {session_id[:8]}")
        # Pre-connect their userbot
        await get_userbot(session_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Verify failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.get("/api/user/status")
async def user_status(session_id: str = Query("")):
    """Check if this web user has a connected Telegram session."""
    if not session_id:
        return JSONResponse({"ok": True, "connected": False})
    try:
        r = get_redis()
        has = bool(r.get(f"relay:session:{session_id}"))
        if has:
            await get_userbot(session_id)  # ensure connected
        return JSONResponse({"ok": True, "connected": has and session_id in _userbots_ready})
    except Exception:
        return JSONResponse({"ok": True, "connected": False})


@web.post("/api/user/logout")
async def user_logout(session_id: str = Form(...)):
    try:
        get_redis().delete(f"relay:session:{session_id}")
    except Exception:
        pass
    c = _userbots.pop(session_id, None)
    _userbots_ready.discard(session_id)
    _handlers_registered.discard(session_id)
    if c:
        try:
            await c.stop()
        except Exception:
            pass
    return JSONResponse({"ok": True})


# ------------------------------------------------------------ Chat API ------
@web.get("/api/chat/bots")
async def chat_bots():
    return JSONResponse({"ok": True, "bots": get_bots()})


@web.post("/api/chat/send")
async def chat_send(session_id: str = Form(...), bot: str = Form(...), text: str = Form(...)):
    client = await get_userbot(session_id)
    if not client:
        return JSONResponse({"ok": False, "error": "Login with your Telegram first."}, status_code=401)
    target = norm_bot(bot)
    if target not in get_bots():
        return JSONResponse({"ok": False, "error": "Bot not available."}, status_code=400)
    try:
        r = get_redis()
        hkey = f"chat:history:{session_id}:{bot_key(target)}"
        # Auto /start on first message to get welcome/default messages
        if r.llen(hkey) == 0 and not text.strip().startswith("/"):
            await client.send_message(target, "/start")
            r.rpush(hkey, json.dumps({"from": "user", "text": "/start", "ts": time.time()}))
            await asyncio.sleep(1)
        r.rpush(hkey, json.dumps({"from": "user", "text": text[:500], "ts": time.time()}))
        r.ltrim(hkey, -200, -1)
        await client.send_message(target, text[:500])
        logger.info(f"[Relay:{session_id[:8]}] -> {target}: {text[:40]}")
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Send failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


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
    client = await get_userbot(session_id)
    if not client:
        return JSONResponse({"ok": False, "error": "Login first."}, status_code=401)
    try:
        r = get_redis()
        cb_json = r.get(f"cb:{cb_id}")
        if not cb_json:
            return JSONResponse({"ok": False, "error": "Button expired."}, status_code=400)
        cb = json.loads(cb_json)
        if cb.get("session") != session_id:
            return JSONResponse({"ok": False, "error": "Not your button."}, status_code=403)
        await client.request_callback_answer(
            chat_id=cb["chat_id"], message_id=cb["msg_id"], callback_data=cb["data"])
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)[:200]}, status_code=500)


@web.get("/api/chat/commands")
async def chat_commands(session_id: str = Query(""), bot: str = Query("")):
    client = await get_userbot(session_id)
    if not client:
        return JSONResponse({"ok": False, "error": "Login first."}, status_code=401)
    try:
        from pyrogram import raw
        target = norm_bot(bot)
        peer = await client.resolve_peer(target)
        result = await client.invoke(raw.functions.bots.GetBotCommands(bot=peer))
        cmds = [{"command": c.command, "description": c.description} for c in result]
        get_redis().setex(f"relay:cmds:{bot_key(target)}", 3600, json.dumps(cmds))
        return JSONResponse({"ok": True, "commands": cmds})
    except Exception as e:
        try:
            cached = get_redis().get(f"relay:cmds:{bot_key(bot)}")
            if cached:
                return JSONResponse({"ok": True, "commands": json.loads(cached), "cached": True})
        except Exception:
            pass
        return JSONResponse({"ok": True, "commands": []})


# ------------------------------------------------------------ Admin --------
@web.post("/api/admin/bots/add")
async def admin_add_bot(bot_username: str = Form(...)):
    b = norm_bot(bot_username)
    if not b:
        return JSONResponse({"ok": False, "error": "Invalid."}, status_code=400)
    get_redis().sadd("relay:bots", b)
    return JSONResponse({"ok": True, "bots": get_bots()})


@web.post("/api/admin/bots/remove")
async def admin_remove_bot(bot_username: str = Form(...)):
    get_redis().srem("relay:bots", norm_bot(bot_username))
    return JSONResponse({"ok": True, "bots": get_bots()})


# ------------------------------------------------------------ Dashboard -----
@web.get("/", response_class=HTMLResponse)
async def dashboard():
    bots = get_bots()
    bots_html = "".join(f"<li>{b}</li>" for b in bots)
    return f"""
    <!DOCTYPE html><html lang="bn"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Multi-Bot Chat Dashboard</title>
    <style>
        body {{ font-family: Arial; background: #0f0f0f; color: #fff; padding: 20px; max-width: 500px; margin: auto; }}
        h1 {{ color: #d4af37; }} .card {{ background: #1a1a1a; padding: 15px; border-radius: 8px; margin: 10px 0; }}
        input, button {{ width: 100%; padding: 12px; margin: 8px 0; border-radius: 8px; border: 1px solid #333; font-size: 16px; box-sizing: border-box; }}
        input {{ background: #222; color: #fff; }} button {{ background: #d4af37; color: #000; font-weight: bold; border: none; cursor: pointer; }}
        a {{ color: #d4af37; }} ul {{ padding-left: 20px; }}
    </style></head>
    <body>
        <h1>🤖 Multi-Bot Chat</h1>
        <div class="card"><h3>Available Bots</h3><ul>{bots_html}</ul></div>
        <div class="card"><h3>➕ Add Bot</h3>
            <form action="/api/admin/bots/add" method="post">
                <input name="bot_username" placeholder="@BotUsername" required>
                <button type="submit">Add Bot</button>
            </form></div>
        <div class="card"><a href="/chat">💬 Open Live Chat →</a></div>
        <div class="card"><a href="/api/status">📊 Status</a></div>
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
        #header { background: #1a1a1a; padding: 12px; border-bottom: 1px solid #333; }
        #header b { color: #d4af37; }
        #bottabs { display: flex; gap: 6px; padding: 8px; background: #141414; overflow-x: auto; border-bottom: 1px solid #333; }
        .btab { padding: 8px 14px; border-radius: 20px; border: 1px solid #444; background: transparent; color: #aaa; cursor: pointer; white-space: nowrap; font-size: 14px; }
        .btab.active { border-color: #d4af37; color: #d4af37; }
        #status { text-align: center; padding: 6px; font-size: 12px; color: #888; }
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
        #loginbox { padding: 20px; text-align: center; }
        #loginbox input { width: 100%; padding: 12px; margin: 8px 0; border-radius: 8px; border: 1px solid #333; background: #222; color: #fff; font-size: 16px; }
        #loginbox button { width: 100%; padding: 12px; border-radius: 8px; border: none; background: #d4af37; color: #000; font-weight: bold; cursor: pointer; margin: 4px 0; }
        #cmdbar { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px; background: #141414; border-top: 1px solid #333; }
    </style></head>
    <body>
        <div id="header"><b>💬 Live Chat</b></div>
        <div id="loginbox">
            <h3>📱 Login with your Telegram</h3>
            <p style="color:#888;font-size:14px;margin:8px 0;">Each user logs in with their own number.<br>Your chats stay private to you.</p>
            <div id="step1">
                <input id="phone" placeholder="+8801XXXXXXXXX">
                <button onclick="sendPhone()">Send OTP</button>
            </div>
            <div id="step2" style="display:none;">
                <input id="otp" placeholder="OTP Code">
                <button onclick="verifyOtp()">Verify & Login</button>
            </div>
            <p id="loginmsg" style="color:#d4af37;margin-top:8px;"></p>
        </div>
        <div id="chatbox" style="display:none;flex:1;flex-direction:column;min-height:0;">
            <div id="bottabs"></div>
            <div id="status"></div>
            <div id="cmdbar" style="display:none;"></div>
            <div id="messages"></div>
            <div id="inputbar">
                <input id="inp" placeholder="Type a message..." autocomplete="off">
                <button onclick="send()">Send</button>
            </div>
        </div>
        <script>
            let sid = localStorage.getItem('tg_sid') || ('u' + Math.random().toString(36).slice(2) + Date.now().toString(36));
            localStorage.setItem('tg_sid', sid);
            let curBot = '', lastTs = 0, bots = [];
            const box = document.getElementById('messages');
            async function checkLogin() {
                const r = await fetch('/api/user/status?session_id=' + sid).then(x => x.json());
                if (r.connected) {
                    document.getElementById('loginbox').style.display = 'none';
                    document.getElementById('chatbox').style.display = 'flex';
                    loadBots();
                }
            }
            async function sendPhone() {
                const p = document.getElementById('phone').value.trim();
                if (!p) return;
                document.getElementById('loginmsg').textContent = 'Sending...';
                const r = await fetch('/api/user/phone', {method:'POST',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:'session_id='+sid+'&phone='+encodeURIComponent(p)}).then(x=>x.json());
                if (r.ok) { document.getElementById('step1').style.display='none';
                    document.getElementById('step2').style.display='block';
                    document.getElementById('loginmsg').textContent='📩 OTP sent! Check Telegram.'; }
                else document.getElementById('loginmsg').textContent='❌ '+r.error;
            }
            async function verifyOtp() {
                const c = document.getElementById('otp').value.trim();
                if (!c) return;
                const r = await fetch('/api/user/verify', {method:'POST',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:'session_id='+sid+'&code='+encodeURIComponent(c)}).then(x=>x.json());
                if (r.ok) checkLogin();
                else document.getElementById('loginmsg').textContent='❌ '+r.error;
            }
            async function loadBots() {
                const d = await fetch('/api/chat/bots').then(x=>x.json());
                bots = d.bots || [];
                const tabs = document.getElementById('bottabs');
                tabs.innerHTML = '';
                bots.forEach((b,i) => {
                    const t = document.createElement('button');
                    t.className = 'btab' + (i===0?' active':'');
                    t.textContent = b;
                    t.onclick = () => switchBot(b);
                    tabs.appendChild(t);
                });
                if (bots.length) switchBot(bots[0]);
            }
            function switchBot(b) {
                curBot = b; lastTs = 0; box.innerHTML = '';
                document.querySelectorAll('.btab').forEach(t=>t.classList.toggle('active', t.textContent===b));
                loadCommands(); poll();
            }
            async function loadCommands() {
                const bar = document.getElementById('cmdbar');
                try {
                    const d = await fetch('/api/chat/commands?session_id='+sid+'&bot='+encodeURIComponent(curBot)).then(x=>x.json());
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
                            const div = document.createElement('div');
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
                            box.appendChild(div);
                        });
                        box.scrollTop = box.scrollHeight;
                    }
                    document.getElementById('status').textContent = '🟢 Chatting with ' + curBot;
                } catch(e) {}
            }
            async function send() {
                const inp = document.getElementById('inp');
                const t = inp.value.trim();
                if (!t || !curBot) return;
                inp.value = '';
                await fetch('/api/chat/send', {method:'POST',
                    headers:{'Content-Type':'application/x-www-form-urlencoded'},
                    body:'session_id='+sid+'&bot='+encodeURIComponent(curBot)+'&text='+encodeURIComponent(t)});
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
            checkLogin();
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
        "bots": get_bots(), "active_sessions": len(_userbots_ready),
    }


@web.get("/health")
async def health():
    return {"ok": True}


# --------------------------------------------------------------- Pyrogram ---
# @mlazone_bot direct messages (Bot API poller fallback)
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


bot = Client(
    config.SESSION_NAME, api_id=config.API_ID, api_hash=config.API_HASH,
    bot_token=config.BOT_TOKEN or None,
)

@bot.on_message(filters.private & filters.text)
async def handle_private_message(client: Client, message: Message):
    logger.info(f"DM from {message.from_user.id}: {message.text[:50]}")
    await message.reply_text(f"Received: {message.text[:100]}")


# ----------------------------------------------------------------- Runner ---
async def main():
    logger.info("Starting services...")
    # Seed default bots
    try:
        r = get_redis()
        for b in DEFAULT_BOTS:
            r.sadd("relay:bots", b)
    except Exception:
        pass
    await bot.start()
    me = await bot.get_me()
    logger.info(f"Bot logged in as @{me.username}")
    asyncio.create_task(bot_api_poller())
    # Restore user sessions from Redis (reconnect personal userbots)
    try:
        r = get_redis()
        for key in r.scan_iter("relay:session:*"):
            sid = key.split(":", 2)[2]
            try:
                await get_userbot(sid)
            except Exception as e:
                logger.warning(f"Restore {sid[:8]} failed: {e}")
    except Exception as e:
        logger.warning(f"Session restore scan failed: {e}")
    logger.info("Starting web server...")
    server = uvicorn.Server(uvicorn.Config(web, host=config.WEB_HOST, port=config.WEB_PORT, log_level="info"))
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
