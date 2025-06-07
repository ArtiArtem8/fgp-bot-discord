"""Main entry point for the FGP Discord bot application.

Handles bot initialization, configuration loading, and event loop management.
Responsible for setting up command handlers, extensions, and core bot functionality.
"""

import asyncio
import logging
import logging.config

from discord import Intents
from discord.ext import commands

from config import (
    DATABASE_FILE,
    DISCORD_BOT_OWNER_ID,
    DISCORD_BOT_PREFIX,
    DISCORD_BOT_TOKEN,
    LOGGING_CONFIG,
)
from core.api_client import APIConfig, MediaAPIClient
from core.database import FileDatabase
from core.exceptions import EnvVarError
from core.file_manager import FileManager

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("FGPBot")

intents = Intents.all()


class FGPBot(commands.Bot):
    """Core bot instance for handling Discord interactions and commands."""

    def __init__(self, file_manager: FileManager, api_client: MediaAPIClient) -> None:
        """Initialize the bot instance with configured settings and file manager."""
        self.file_manager = file_manager
        self.api_client = api_client
        super().__init__(command_prefix=DISCORD_BOT_PREFIX, intents=intents)

    async def setup_hook(self) -> None:
        """Async initialization hook for pre-connection setup.

        - Extension loading
        - Global command tree synchronization.
        """
        extensions: list[str] = ["cogs.local_cog", "cogs.api_cog", "cogs.listener_cog"]
        for ext in extensions:
            await self.load_extension(ext)
        cmds = await self.tree.sync()
        logger.info("Synced %d global commands", len(cmds))
        logger.debug("Synced commands: %s", cmds)


async def main() -> None:
    """Async function to start the bot and manage resources."""
    logger.info("Starting FGPBot")
    config = APIConfig()

    if DISCORD_BOT_TOKEN is None:
        msg = "DISCORD_BOT_TOKEN"
        raise EnvVarError(msg)

    db = FileDatabase(DATABASE_FILE)
    api_client = MediaAPIClient(config)

    await db.connect()
    try:
        file_manager = FileManager(db)
        bot = FGPBot(file_manager=file_manager, api_client=api_client)
        bot.owner_id = DISCORD_BOT_OWNER_ID
        async with bot:
            await bot.start(DISCORD_BOT_TOKEN)
    finally:
        await db.conn.close()
        logger.debug("Database connection closed")
        await api_client.close()
        logger.debug("API Client connection closed")


if __name__ == "__main__":
    asyncio.run(main())
