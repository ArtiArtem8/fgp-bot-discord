"""Configuration for for fgp-bot-discord."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ENCODING = "utf-8"

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_BOT_PREFIX = "!fgp"
DISCORD_BOT_OWNER_ID = int(os.getenv("DISCORD_BOT_OWNER_ID", "0"))
ROOT_DIR = Path(__file__).parent

REACT_CHANCE = 160  # 0.625%
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 mega bytes

DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILENAME = os.getenv("LOG_FILENAME", "fgp_bot.log")

LOG_FILE = DATA_DIR / LOG_FILENAME
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

DATABASE_FILE = DATA_DIR / "file.db"

MEME_DIR = DATA_DIR / "memes"
MEME_DIR.mkdir(parents=True, exist_ok=True)

PRIVATE_DIR = DATA_DIR / "private"
PRIVATE_DIR.mkdir(parents=True, exist_ok=True)

CATEGORY_MAP: dict[Path, str] = {
    MEME_DIR: "meme",
    PRIVATE_DIR: "private",
}


CONVERTED_DIR = DATA_DIR / "converted"
CONVERTED_DIR.mkdir(parents=True, exist_ok=True)


LOGGING_CONFIG: dict[str, object] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "{asctime} {levelname:<8} [{name}]: {message}",
            "datefmt": "%Y-%m-%d %H:%M:%S",
            "style": "{",
        },
        "simple": {
            "format": "[{levelname:<8}] {name}: {message}",
            "style": "{",
        },
    },
    "handlers": {
        "file_handler": {
            "class": "logging.FileHandler",
            "encoding": "utf-8",
            "formatter": "detailed",
            "mode": "a",
            "filename": LOG_FILE,
            "level": LOG_LEVEL,
        },
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": LOG_LEVEL,
        },
    },
    "root": {
        "handlers": ["file_handler", "console"],
        "level": LOG_LEVEL,
    },
    "loggers": {
        "discord": {
            "handlers": ["file_handler", "console"],
            "level": ("ERROR", "INFO")[LOG_LEVEL == "DEBUG"],
            "propagate": False,
        },
        "cogs": {
            "handlers": ["file_handler", "console"],
            "level": ("INFO", "DEBUG")[LOG_LEVEL == "DEBUG"],
            "propagate": False,
        },
        "aiosqlite": {
            "handlers": ["file_handler"],
            "level": ("ERROR", "DEBUG")[LOG_LEVEL == "DEBUG"],
            "propagate": False,
        },
    },
}
