"""
Combined single-process app: FastAPI dashboard + Pyrogram userbot in ONE slot.

Run: python combined.py
(Uses the same config.py + requirements.txt as the split version.)

Endpoints:
- GET  /           - Dashboard (simple HTML, OTP form)
- POST /api/otp    - Submit OTP manually (matched via Redis)
- GET  /api/status - System status
- GET  /health     - Health check
"""
import asyncio
import logging
import os

import redis
import uvicorn
from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pyrogram import Client, filters
from pyrogram.types import Message

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("combined")

# ---------------------------------------------------------------- FastAPI ---
web = FastAPI(title="Lamon Telegram Userbot System")


def get_redis():
    return redis.Redis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        password=config.REDIS_PASSWORD or None,
        db=config.REDIS_DB,
        decode_responses=True,
    )


@web.get("/", response_class=HTMLResponse)
async def dashboard(ok: str = ""):
    success_banner = ""
    if ok == "1":
        success_banner = '<div style="background:#1a4d1a;padding:15px;border-radius:8px;margin-bottom:15px;border:1px solid #2d7a2d;">✅ OTP received! Userbot will process it.</div>'
    return f"""
    <!DOCTYPE html>
    <html lang="bn">
    <head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
    <title>Lamon Userbot Dashboard</title>
    <style>
        body {{ font-family: Arial; background: #0f0f0f; color: #fff; padding: 20px; max-width: 500px; margin: auto; }}
        h1 {{ color: #d4af37; }}
        input, button {{ width: 100%; padding: 12px; margin: 8px 0; border-radius: 8px; border: 1px solid #333; font-size: 16px; }}
        input {{ background: #1a1a1a; color: #fff; }}
        button {{ background: #d4af37; color: #000; font-weight: bold; cursor: pointer; border: none; }}
        .status {{ background: #1a1a1a; padding: 15px; border-radius: 8px; margin-top: 15px; }}
    </style></head>
    <body>
        <h1>🤖 Lamon Userbot</h1>
        {success_banner}
        <p>Telegram OTP manually enter করুন:</p>
        <form action="/api/otp" method="post">
            <input name="phone" placeholder="Phone (e.g. +8801XXXXXXXXX)" required>
            <input name="otp" placeholder="OTP Code" required>
            <button type="submit">Submit OTP</button>
        </form>
        <div class="status" id="status">Loading status...</div>
        <script>
            fetch('/api/status').then(r => r.json()).then(d => {{
                document.getElementById('status').innerHTML =
                    '<b>Status:</b> ' + d.status + '<br><b>Bot:</b> @' + d.bot + '<br><b>Redis:</b> ' + d.redis;
            }});
        </script>
    </body></html>
    """


@web.post("/api/otp")
async def submit_otp(phone: str = Form(...), otp: str = Form(...)):
    """User manually submits their Telegram OTP. Stored in Redis for userbot matching."""
    try:
        r = get_redis()
        r.setex(f"otp:{phone}", 300, otp.strip())
    except Exception:
        pass
    # Redirect back to dashboard with success flag (POST-redirect-GET)
    return RedirectResponse(url="/?ok=1", status_code=303)


@web.get("/api/status")
async def system_status():
    """Check system health."""
    redis_ok = "unknown"
    try:
        r = get_redis()
        r.ping()
        redis_ok = "connected"
    except Exception as e:
        redis_ok = f"error: {e}"
    return {
        "status": "running",
        "bot": config.BOT_USERNAME,
        "redis": redis_ok,
        "userbot": "running" if bot.is_connected else "disconnected",
    }


@web.get("/health")
async def health():
    return {"ok": True}


# --------------------------------------------------------------- Pyrogram ---
# Session string (for headless login) or bot token — set via env vars
_session_string = os.getenv("SESSION_STRING", "")
_bot_token = config.BOT_TOKEN

bot = Client(
    config.SESSION_NAME,
    api_id=config.API_ID,
    api_hash=config.API_HASH,
    session_string=_session_string or None,
    bot_token=_bot_token or None,
)


@bot.on_message(filters.private & filters.text)
async def handle_private_message(client: Client, message: Message):
    """Handle incoming private messages."""
    logger.info(f"Message from {message.from_user.id}: {message.text[:50]}")
    # TODO: Route to the correct web user via Redis session mapping
    # For now, echo back
    await message.reply_text(f"Received: {message.text[:100]}")


# ----------------------------------------------------------------- Runner ---
async def main():
    logger.info("Starting Pyrogram userbot...")
    await bot.start()
    logger.info("Userbot connected! Starting web server...")
    server = uvicorn.Server(
        uvicorn.Config(
            web,
            host=config.WEB_HOST,
            port=config.WEB_PORT,
            log_level="info",
        )
    )
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
