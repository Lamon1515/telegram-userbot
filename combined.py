"""
Live chat relay: Web users <-> @FastDpHouseBot via Pyrogram userbot.

Architecture:
- Userbot (Pyrogram user session) talks to @FastDpHouseBot
- FastAPI provides chat API + live web UI
- Redis maps web sessions to conversations

Endpoints:
- GET  /           - Dashboard (session setup + status)
- GET  /chat       - Live chat UI (embeddable)
- POST /api/session/phone  - Start userbot login (send OTP)
- POST /api/session/verify - Verify OTP, save session string
- POST /api/chat/send      - Web user sends message -> @FastDpHouseBot
- GET  /api/chat/messages  - Get new messages from @FastDpHouseBot
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

TARGET_BOT = "@FastDpHouseBot"

# ---------------------------------------------------------------- FastAPI ---
web = FastAPI(title="Lamon Live Chat Relay")


def get_redis():
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD or None,
        db=config.REDIS_DB,
        decode_responses=True,
    )


# ------------------------------------------------------------ Userbot -------
_session_string = os.getenv("SESSION_STRING", "")
_bot_token = config.BOT_TOKEN

# Main bot client (for direct @mlazone_bot messages)
bot = Client(
    config.SESSION_NAME,
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    session_string=_session_string or None,
    bot_token=_bot_token or None,
)

# Userbot client for @FastDpHouseBot relay (needs user session)
userbot = None
userbot_ready = False


async def init_userbot():
    """Initialize userbot if session string is available."""
    global userbot, userbot_ready
    ss = os.getenv("SESSION_STRING", "")
    if not ss:
        logger.info("No SESSION_STRING — userbot relay disabled")
        return
    try:
        userbot = Client(
            "relay_userbot",
            api_id=config.API_ID,
            api_hash=config.API_HASH,
            session_string=ss,
        )
        await userbot.start()
        me = await userbot.get_me()
        logger.info(f"Relay userbot logged in as @{me.username} (id={me.id})")
        userbot_ready = True

        @userbot.on_message(filters.chat(TARGET_BOT.strip("@")))
        async def handle_target_reply(client, message: Message):
            """When @FastDpHouseBot replies, store for web UI."""
            text = message.text or message.caption or "[media]"
            logger.info(f"[Relay] Reply from {TARGET_BOT}: {text[:80]}")
            r = get_redis()
            # Store in chat history
            msg_data = json.dumps({
                "from": "bot",
                "text": text,
                "ts": time.time(),
            })
            r.rpush("chat:history", msg_data)
            r.ltrim("chat:history", -100, -1)  # Keep last 100
            # Notify web UI via pub/sub
            r.publish("chat:updates", msg_data)

    except Exception as e:
        logger.error(f"Userbot init failed: {e}")
        userbot_ready = False


# ------------------------------------------------------------ Chat API ------
@web.post("/api/chat/send")
async def chat_send(text: str = Form(...), session_id: str = Form("")):
    """Web user sends a message -> forwarded to @FastDpHouseBot via userbot."""
    if not userbot_ready or not userbot:
        return JSONResponse({"ok": False, "error": "Userbot not connected. Complete session setup first."}, status_code=503)
    try:
        # Store user message in history
        r = get_redis()
        msg_data = json.dumps({
            "from": "user",
            "text": text[:500],
            "ts": time.time(),
            "session": session_id,
        })
        r.rpush("chat:history", msg_data)
        r.ltrim("chat:history", -100, -1)
        # Forward to @FastDpHouseBot
        await userbot.send_message(TARGET_BOT, text[:500])
        logger.info(f"[Relay] User -> {TARGET_BOT}: {text[:50]}")
        return JSONResponse({"ok": True})
    except Exception as e:
        logger.error(f"Chat send failed: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@web.get("/api/chat/messages")
async def chat_messages(since: float = Query(0)):
    """Get chat messages newer than `since` timestamp (polling)."""
    try:
        r = get_redis()
        all_msgs = r.lrange("chat:history", 0, -1)
        msgs = [json.loads(m) for m in all_msgs]
        new_msgs = [m for m in msgs if m.get("ts", 0) > since]
        return JSONResponse({"ok": True, "messages": new_msgs})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@web.get("/api/chat/status")
async def chat_status():
    """Check if the relay userbot is ready."""
    return JSONResponse({
        "ok": True,
        "relay_ready": userbot_ready,
        "target": TARGET_BOT,
    })


# ------------------------------------------------------------ Session setup -
# Temporary storage for phone auth (in-memory, single process)
_pending_auth = {}


@web.post("/api/session/phone")
async def session_phone(phone: str = Form(...)):
    """Step 1: Send OTP to phone for userbot login."""
    if not userbot_ready:
        try:
            tmp = Client(
                "auth_tmp",
                api_id=config.API_ID,
                api_hash=config.API_HASH,
                workdir="/tmp",
            )
            await tmp.connect()
            sent = await tmp.send_code(phone.strip())
            _pending_auth["client"] = tmp
            _pending_auth["phone"] = phone.strip()
            _pending_auth["hash"] = sent.phone_code_hash
            logger.info(f"OTP sent to {phone.strip()}")
        except Exception as e:
            logger.error(f"Send code failed: {e}")
            return RedirectResponse(url="/?err=" + str(e)[:100], status_code=303)
    return RedirectResponse(url="/?otp=sent", status_code=303)


@web.post("/api/session/verify")
async def session_verify(code: str = Form(...)):
    """Step 2: Verify OTP, generate and save session string."""
    tmp = _pending_auth.get("client")
    if not tmp:
        return RedirectResponse(url="/?err=no_auth", status_code=303)
    try:
        phone = _pending_auth["phone"]
        phone_hash = _pending_auth["hash"]
        await tmp.sign_in(phone, phone_hash, code.strip())
        ss = await tmp.export_session_string()
        await tmp.disconnect()
        _pending_auth.clear()
        os.environ["SESSION_STRING"] = ss
        logger.info("Session string generated!")
        await init_userbot()
        if userbot_ready:
            return RedirectResponse(url="/?ok=connected", status_code=303)
        return RedirectResponse(url="/?err=relay_failed", status_code=303)
    except Exception as e:
        logger.error(f"Verify failed: {e}")
        return RedirectResponse(url="/?err=" + str(e)[:100], status_code=303)


# ------------------------------------------------------------ Dashboard -----
@web.get("/", response_class=HTMLResponse)
async def dashboard(ok: str = "", otp: str = "", err: str = ""):
    banner = ""
    if ok == "connected":
        banner = '<div style="background:#1a4d1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d7a2d;">✅ Userbot connected! Relay is live.</div>'
    elif otp == "sent":
        banner = '<div style="background:#1a3a4d;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d6a7a;">📩 OTP sent! Check Telegram and enter the code below.</div>'
    elif err:
        banner = f'<div style="background:#4d1a1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #7a2d2d;">❌ Error: {err}</div>'
    relay = "🟢 Connected" if userbot_ready else "🔴 Not connected"
    return f"""
    <!DOCTYPE html>
    <html lang="bn"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Live Chat Relay Dashboard</title>
    <style>
        body {{ font-family: Arial; background: #0f0f0f; color: #fff; padding: 20px; max-width: 500px; margin: auto; }}
        h1 {{ color: #d4af37; }} .card {{ background: #1a1a1a; padding: 15px; border-radius: 8px; margin: 10px 0; }}
        input, button {{ width: 100%; padding: 12px; margin: 8px 0; border-radius: 8px; border: 1px solid #333; font-size: 16px; box-sizing: border-box; }}
        input {{ background: #222; color: #fff; }} button {{ background: #d4af37; color: #000; font-weight: bold; border: none; cursor: pointer; }}
        a {{ color: #d4af37; }}
    </style></head>
    <body>
        <h1>💬 Live Chat Relay</h1>
        {banner}
        <div class="card"><b>Target:</b> {TARGET_BOT}<br><b>Relay:</b> {relay}</div>
        <div class="card">
            <h3>🔑 Userbot Login</h3>
            <p>Step 1: Enter phone to receive OTP</p>
            <form action="/api/session/phone" method="post">
                <input name="phone" placeholder="+8801XXXXXXXXX" required>
                <button type="submit">Send OTP</button>
            </form>
            <p>Step 2: Enter OTP code</p>
            <form action="/api/session/verify" method="post">
                <input name="code" placeholder="OTP Code" required>
                <button type="submit">Verify & Connect</button>
            </form>
        </div>
        <div class="card"><a href="/chat">💬 Open Live Chat →</a></div>
        <div class="card"><a href="/api/status">📊 Status API</a></div>
    </body></html>
    """


@web.get("/chat", response_class=HTMLResponse)
async def chat_ui():
    """Embeddable live chat UI."""
    return """
    <!DOCTYPE html>
    <html lang="bn"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Live Chat</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; }
        body { font-family: Arial; background: #0f0f0f; color: #fff; height: 100vh; display: flex; flex-direction: column; }
        #header { background: #1a1a1a; padding: 12px; text-align: center; border-bottom: 1px solid #333; }
        #header b { color: #d4af37; }
        #messages { flex: 1; overflow-y: auto; padding: 12px; display: flex; flex-direction: column; gap: 8px; }
        .msg { max-width: 80%; padding: 10px 14px; border-radius: 12px; word-wrap: break-word; }
        .msg.user { align-self: flex-end; background: #d4af37; color: #000; }
        .msg.bot { align-self: flex-start; background: #222; border: 1px solid #333; }
        #inputbar { display: flex; padding: 10px; gap: 8px; background: #1a1a1a; border-top: 1px solid #333; }
        #inputbar input { flex: 1; padding: 12px; border-radius: 8px; border: 1px solid #333; background: #222; color: #fff; font-size: 16px; }
        #inputbar button { padding: 12px 20px; border-radius: 8px; border: none; background: #d4af37; color: #000; font-weight: bold; cursor: pointer; }
        #status { text-align: center; padding: 6px; font-size: 12px; color: #888; }
    </style></head>
    <body>
        <div id="header"><b>💬 Live Chat</b> with @FastDpHouseBot</div>
        <div id="status">Connecting...</div>
        <div id="messages"></div>
        <div id="inputbar">
            <input id="inp" placeholder="Type a message..." autocomplete="off">
            <button onclick="send()">Send</button>
        </div>
        <script>
            let lastTs = 0;
            const box = document.getElementById('messages');
            const status = document.getElementById('status');
            async function poll() {
                try {
                    const r = await fetch('/api/chat/messages?since=' + lastTs);
                    const d = await r.json();
                    if (d.ok && d.messages.length) {
                        d.messages.forEach(m => {
                            lastTs = Math.max(lastTs, m.ts);
                            const div = document.createElement('div');
                            div.className = 'msg ' + m.from;
                            div.textContent = m.text;
                            box.appendChild(div);
                        });
                        box.scrollTop = box.scrollHeight;
                    }
                    const s = await fetch('/api/chat/status').then(x => x.json());
                    status.textContent = s.relay_ready ? '🟢 Connected' : '🔴 Relay offline — admin setup needed';
                } catch(e) { status.textContent = '⚠️ Connection error'; }
            }
            async function send() {
                const inp = document.getElementById('inp');
                const t = inp.value.trim();
                if (!t) return;
                inp.value = '';
                await fetch('/api/chat/send', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/x-www-form-urlencoded'},
                    body: 'text=' + encodeURIComponent(t)
                });
                poll();
            }
            document.getElementById('inp').addEventListener('keypress', e => { if (e.key === 'Enter') send(); });
            setInterval(poll, 2000); poll();
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
        "status": "running",
        "bot": config.BOT_USERNAME,
        "redis": redis_ok,
        "relay_ready": userbot_ready,
        "target": TARGET_BOT,
    }


@web.get("/health")
async def health():
    return {"ok": True}


# --------------------------------------------------------------- Pyrogram ---
# (Bot API poller for @mlazone_bot direct messages — kept from before)
import aiohttp
_bot_api_token = _bot_token
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
                        await bot_api_send(chat_id, "👋 Hello! I'm @mlazone_bot\n\n💬 Use /chat for live chat with @FastDpHouseBot")
                    else:
                        await bot_api_send(chat_id, f"Received: {text[:100]}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Poller error: {e}")
                await asyncio.sleep(5)


@bot.on_message(filters.private & filters.text)
async def handle_private_message(client: Client, message: Message):
    logger.info(f"Message from {message.from_user.id}: {message.text[:50]}")
    await message.reply_text(f"Received: {message.text[:100]}")


# ----------------------------------------------------------------- Runner ---
async def main():
    logger.info("Starting services...")
    await bot.start()
    me = await bot.get_me()
    logger.info(f"Bot logged in as @{me.username}")
    asyncio.create_task(bot_api_poller())
    await init_userbot()
    logger.info("Starting web server...")
    server = uvicorn.Server(uvicorn.Config(web, host=config.WEB_HOST, port=config.WEB_PORT, log_level="info"))
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
