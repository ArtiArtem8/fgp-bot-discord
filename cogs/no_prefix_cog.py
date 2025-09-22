"""Cog to block messages without a prefix and suggest slash commands.

## Example Usage
.. code:: python

    async def setup_bot():
        bot = commands.Bot(command_prefix="!")
        await bot.add_cog(PrefixBlockerCog(bot))

    # The cog will automatically:
    # - Detect messages using prefix commands
    # - Reply with instructions to use slash commands
    # - Provide fuzzy-matched command suggestions
"""

import logging

from discord import Message
from discord.ext import commands
from fuzzywuzzy.process import extractOne  # type: ignore  # noqa: PGH003


class PrefixBlockerCog(commands.Cog):
    """Blocks prefix commands and guides users toward slash commands."""

    def __init__(self, bot: commands.Bot) -> None:
        """Initialize the PrefixBlockerCog with bot instance.

        :param commands.Bot bot: Discord bot instance to monitor for prefix commands
        """
        self.bot = bot
        self.logger = logging.getLogger("PrefixBlockerCog")

    @commands.Cog.listener()
    async def on_message(self, message: Message) -> None:
        """Process incoming messages and respond to prefix command attempts.

        This listener detects when users try to use prefix commands and provides
        guidance on using slash commands instead. Includes fuzzy matching to
        suggest the most likely intended slash command.

        :param Message message: Incoming Discord message to analyze
        """
        if message.author.bot:
            return

        prefixes = await self.bot.get_prefix(message)
        prefixes = [prefixes] if isinstance(prefixes, str) else prefixes

        if not any((message.content.startswith(i), pref := i)[0] for i in prefixes):
            return

        raw_content = message.content.removeprefix(pref).strip()

        slash_commands = [cmd.name for cmd in self.bot.tree.get_commands()]

        suggestion = None
        if slash_commands:
            best_match: tuple[str, int] = extractOne(raw_content, slash_commands)  # pyright: ignore[reportUnknownVariableType, reportAssignmentType]

            threshold = 25
            if best_match and best_match[1] >= threshold:
                suggestion = best_match[0]

        response = "Префиксы убраны; воспользуйтесь слэш-командами."
        if suggestion:
            response += f" (возможно вы имели в виду `/{suggestion}`)"

        try:
            await message.reply(response, mention_author=False, delete_after=30)
        except Exception:
            self.logger.exception("Failed to send prefix warning")


async def setup(bot: commands.Bot) -> None:
    """Add the PrefixBlockerCog to the bot instance.

    :param `commands.Bot` bot: Discord bot instance to receive the cog
    """
    await bot.add_cog(PrefixBlockerCog(bot))
