"""Discord listeners for handling bot events and message reactions."""

import logging
import secrets

from discord import Emoji, Message
from discord.ext import commands

from config import REACT_CHANCE

logger = logging.getLogger("ListenerCog")


class ListenerCog(commands.Cog):
    """Event handler cog for core bot functionality and message interactions."""

    def __init__(self, bot: commands.Bot) -> None:
        """Initialize the ListenerCog with the bot instance.

        :param `commands.Bot` bot: Discord bot instance to associate with this cog
        """
        self.bot = bot

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        """Handle the bot's ready event, logging login information."""
        if self.bot.user is None:
            logger.error("Bot is not logged in")
            return
        logger.info("Logged in as %s (ID: %s)", self.bot.user, self.bot.user.id)

    @commands.Cog.listener()
    async def on_message(self, message: Message) -> None:
        """Handle incoming messages and randomly react with a static emoji.

        :param `Message` message: Discord message object to process
        """
        if message.author == self.bot.user:
            return
        if secrets.randbelow(REACT_CHANCE) < 1:
            logger.info("%s Повезло с шансом %0.2f", message.author, 1 / REACT_CHANCE)
            emoji_pool = self._get_available_emojis(message)
            await message.add_reaction(secrets.choice(emoji_pool or ["👍"]))

        await self.bot.process_commands(message)  # CRITICAL: DO NOT DELETE

    def _get_available_emojis(self, message: Message) -> list[Emoji]:
        """Get available static emojis from appropriate source.

        :param `Message` message: Message context to determine emoji source
        :return: List of available static emojis
        """
        return [
            emoji
            for emoji in (message.guild.emojis if message.guild else self.bot.emojis)
            if not emoji.animated
        ]


async def setup(bot: commands.Bot) -> None:
    """Add the ListenerCog to the bot instance.

    :param `commands.Bot` bot: Discord bot instance to receive the cog
    """
    await bot.add_cog(ListenerCog(bot))
