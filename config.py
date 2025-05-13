"""Docstring for config."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ENCODING = "utf-8"

DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
DISCORD_BOT_PREFIX = "!fgp"

ROOT_DIR = Path(__file__).parent

DATA_DIR = ROOT_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = DATA_DIR / "fgp_bot.log"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


LOGGING_CONFIG: dict[str, object] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "detailed": {
            "format": "%(asctime)s %(levelname)s [%(name)s]: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "%(levelname)s: %(name)s: %(message)s",
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
            "handlers": ["file_handler"],
            "level": ("WARNING", "DEBUG")[LOG_LEVEL == "DEBUG"],
            "propagate": False,
        },
        "cogs": {
            "handlers": ["file_handler", "console"],
            "level": ("INFO", "DEBUG")[LOG_LEVEL == "DEBUG"],
            "propagate": False,
        },
    },
}
