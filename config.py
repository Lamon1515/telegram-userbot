"""Configuration - loads from environment variables."""
import os

# Telegram API (from my.telegram.org)
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "")

# Bot token (from @BotFather)
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "mlazone_bot")

# Redis (Botkeep Redis slot)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Web server
WEB_HOST = os.getenv("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.getenv("WEB_PORT", "8000"))

# Session name for Pyrogram userbot
SESSION_NAME = os.getenv("SESSION_NAME", "lamon_userbot")
