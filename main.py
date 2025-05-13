"""Docstring for main."""

import asyncio
import logging

from discord import Intents
from discord.ext import commands

from config import DISCORD_BOT_PREFIX, DISCORD_BOT_TOKEN

logger = logging.getLogger("FGPBot")

intents = Intents.default()


class FGPBot(commands.Bot):
    """docstring."""

    def __init__(self) -> None:
        """Docstring for __init__."""
        super().__init__(command_prefix=DISCORD_BOT_PREFIX, intents=intents)

    async def setup_hook(self) -> None:
        """Docstring for setup_hook."""
        extentions: list[str] = []
        for ext in extentions:
            await self.load_extension(ext)
        await self.tree.sync()
        logger.info("Application commands synced")


if __name__ == "__main__":

    async def _main() -> None:
        async with FGPBot() as bot:
            await bot.start(DISCORD_BOT_TOKEN or "Token_is_missing")

    asyncio.run(_main())
