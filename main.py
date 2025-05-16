"""Main entry point for the FGP Discord bot application.

Handles bot initialization, configuration loading, and event loop management.
Responsible for setting up command handlers, extensions, and core bot functionality.
"""

import asyncio
import logging
import logging.config

from discord import Intents
from discord.ext import commands

from config import DATABASE_FILE, DISCORD_BOT_PREFIX, DISCORD_BOT_TOKEN, LOGGING_CONFIG
from core.database import FileDatabase
from core.exceptions import EnvVarError
from core.file_manager import FileManager

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("FGPBot")

intents = Intents.all()


class FGPBot(commands.Bot):
    """Core bot instance for handling Discord interactions and commands."""

    def __init__(self) -> None:
        """Initialize the bot instance with configured settings."""
        super().__init__(command_prefix=DISCORD_BOT_PREFIX, intents=intents)

    async def setup_hook(self) -> None:
        """Async initialization hook for pre-connection setup.

        - Extension loading
        - Global command tree synchronization.
        """
        extentions: list[str] = []
        for ext in extentions:
            await self.load_extension(ext)
        await self.tree.sync()
        logger.info("Application commands synced")


if __name__ == "__main__":
    logger.info("Starting FGPBot")

    db = FileDatabase(DATABASE_FILE)

    if DISCORD_BOT_TOKEN is None:
        var = "DISCORD_BOT_TOKEN"
        raise EnvVarError(var)

    async def _main() -> None:
        try:
            await db.connect()
            fm = FileManager(db)
            await fm.load_all_files()
            await fm.compress_all_large_files()
        finally:
            await db.conn.close()
        # async with FGPBot() as bot:
        #     await bot.start(DISCORD_BOT_TOKEN)

    asyncio.run(_main())
