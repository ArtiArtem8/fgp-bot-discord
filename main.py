"""Main entry point for the FGP Discord bot application.

Handles bot initialization, configuration loading, and event loop management.
Responsible for setting up command handlers, extensions, and core bot functionality.
"""

import asyncio
import logging
import logging.config
from collections.abc import Coroutine

from discord import Intents
from discord.ext import commands

from config import (
    DATABASE_FILE,
    DISCORD_BOT_OWNER_ID,
    DISCORD_BOT_PREFIX,
    DISCORD_BOT_TOKEN,
    LOGGING_CONFIG,
)
from core import (
    APIConfig,
    BotNotInitializedError,
    EnvVarError,
    FileDatabase,
    FileManager,
    MediaAPIClient,
)

logging.config.dictConfig(LOGGING_CONFIG)
logger = logging.getLogger("FGPBot")

intents = Intents.all()


class FGPBot(commands.Bot):
    """Core bot instance for handling Discord interactions and commands."""

    def __init__(self) -> None:
        """Initialize the bot instance with configured settings and file manager."""
        self._db: FileDatabase | None = None
        self._file_manager: FileManager | None = None
        self._api_client: MediaAPIClient | None = None
        super().__init__(command_prefix=DISCORD_BOT_PREFIX, intents=intents)

    @property
    def db(self) -> FileDatabase:
        """Get database instance.

        :return: Initialized database instance.
        :rtype: FileDatabase
        :raises BotNotInitializedError: If accessed before initialization.
        """
        if self._db is None:
            msg = "Database"
            raise BotNotInitializedError(msg)
        return self._db

    @property
    def file_manager(self) -> FileManager:
        """Get file manager instance.

        :return: Initialized file manager instance.
        :rtype: FileManager
        :raises BotNotInitializedError: If accessed before initialization.
        """
        if self._file_manager is None:
            msg = "FileManager"
            raise BotNotInitializedError(msg)
        return self._file_manager

    @property
    def api_client(self) -> MediaAPIClient:
        """Get API client instance.

        :return: Initialized API client instance.
        :rtype: MediaAPIClient
        :raises BotNotInitializedError: If accessed before initialization.
        """
        if self._api_client is None:
            msg = "APIClient"
            raise BotNotInitializedError(msg)
        return self._api_client

    async def setup_hook(self) -> None:
        """Async initialization hook for pre-connection setup."""
        try:
            logger.info("Loading extensions")

            config = APIConfig.from_env()
            self._api_client = MediaAPIClient(config)

            self._db = FileDatabase(DATABASE_FILE)
            await self._db.connect()

            self._file_manager = FileManager(self.db)

            extensions = [
                "cogs.local_cog",
                "cogs.api_cog",
                "cogs.listener_cog",
                "cogs.no_prefix_cog",
            ]
            for extension in extensions:
                await self.load_extension(extension)
                logger.debug("Loaded extension: %s", extension)
            logger.info("Setup complete")
            # cmds = await self.tree.sync()
            # logger.info("Synced %d global commands", len(cmds))
            # logger.debug("Synced commands: %s", cmds)
        except Exception as e:
            logger.exception("Error during setup: %s", e)
            await self.cleanup_resources()
            raise

    async def cleanup_resources(self) -> None:
        """Cleanup all bot resources concurrently.

        Closes database and API client connections safely.
        Safe to call multiple times.
        """
        cleanup_tasks: list[Coroutine[None, None, None]] = []

        if self._db is not None:
            cleanup_tasks.append(self.db.close())

        if self._api_client is not None:
            cleanup_tasks.append(self.api_client.close())

        if cleanup_tasks:
            results: list[BaseException | None] = await asyncio.gather(
                *cleanup_tasks,
                return_exceptions=True,
            )
            for res in results:
                if isinstance(res, Exception):
                    logger.error("Error during cleanup: %s", res)

        logger.info("Resource cleanup complete")

    async def close(self) -> None:
        """Override close method to ensure proper cleanup."""
        await self.cleanup_resources()
        await super().close()


async def main() -> None:
    """Async function to start the bot and manage resources."""
    logger.info("Starting FGPBot")

    if DISCORD_BOT_TOKEN is None:
        msg = "DISCORD_BOT_TOKEN"
        raise EnvVarError(msg)

    bot = FGPBot()
    bot.owner_id = DISCORD_BOT_OWNER_ID

    async with bot:
        await bot.start(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown requested")
    except Exception:
        logger.exception("Fatal error occurred")
        raise
