"""Discord cog providing API-based commands for interacting with booru-like service."""

import io
import logging
import secrets

import discord
from discord import Interaction, app_commands
from discord.ext import commands

from core.api_client import ContentParams
from core.enums import Category, DateRange, FileType, Rating, SortOrder
from main import FGPBot

logger = logging.getLogger("ListenerCog")
MAX_MESSAGE_LENGTH = 1990


class ApiCog(commands.Cog):
    """Provides commands for fetching media and tags from a booru-like API."""

    def __init__(self, bot: FGPBot) -> None:
        """Initialize the ApiCog with a reference to the bot instance.

        :param FGPBot bot: The bot instance this cog is attached to
        """
        self.bot = bot

    def _get_random_emoji(self, guild: discord.Guild | None) -> discord.Emoji:
        """Retrieve a random static emoji from the guild or the bot's emojis.

        :param discord.Guild | None guild: The guild to fetch emojis from,
        or None for bot emojis.
        :return discord.Emoji: A randomly selected static emoji.
        """
        return secrets.choice(
            [
                emoji
                for emoji in (guild.emojis if guild else self.bot.emojis)
                if not emoji.animated and emoji.available
            ],
        )

    @app_commands.command(name="e", description="Get a picture with advanced filtering")
    @app_commands.describe(
        tags="Tags to search for (space-separated)",
        rating=("Rating filter"),
        file_type=("File type filter (webm - WebM video)"),
        sort_order=("Sorting method\n(+ _asc suffixes for reverse)"),
        date_range=("Filter posts from the last X days"),
    )
    async def posts(  # noqa: PLR0913
        self,
        interaction: Interaction,
        tags: str = "",
        rating: Rating | None = None,
        file_type: FileType | None = None,
        sort_order: SortOrder | None = SortOrder.SCORE,
        date_range: DateRange | None = DateRange.WEEK,
    ) -> None:
        """Fetch and send a random media post from the API based on search criteria.

        :param Interaction interaction: Discord interaction object
        :param str tags: Space-separated search tags (default: "")
        :param Rating | None rating: Content rating filter (default: None)
        :param FileType | None file_type: Media type filter (default: None)
        :param SortOrder | None sort_order: Result ordering method (default: Score)
        :param DateRange | None date_range: Time range filter (default: Week)
        """
        if interaction.channel is None or isinstance(
            interaction.channel,
            (discord.GroupChannel, discord.Thread),
        ):
            msg = f"You are not in channel {self._get_random_emoji(interaction.guild)}"
            await interaction.response.send_message(msg, ephemeral=True)
            return
        if (
            not isinstance(interaction.channel, discord.DMChannel)
            and not interaction.channel.nsfw
        ):
            emoji = self._get_random_emoji(interaction.guild)
            msg = f"You are not in an NSFW channel. {emoji}"
            await interaction.response.send_message(msg, ephemeral=True)
            return
        if interaction.user.id != self.bot.owner_id:
            await interaction.response.send_message(
                "Вы не в белом списке",
                ephemeral=True,
            )
            return
        await interaction.response.defer(thinking=True)
        try:
            params = ContentParams(
                tags=tags.split(),
                rating=rating,
                file_type=file_type,
                sort_order=sort_order,
                date_range=date_range,
            )
            res = await self.bot.api_client.get_content(content_params=params)
            random_res = secrets.choice(res.posts)
            data = await self.bot.api_client.download_file(random_res.file.url)
            file_buffer = io.BytesIO(data)
            await interaction.followup.send(
                file=discord.File(
                    file_buffer,
                    filename=f"{random_res.file.hash}.{random_res.file.extension}",
                ),
            )
        except Exception:
            logger.exception("Failed to get posts")
            await interaction.followup.send("Failed to get posts")

    @app_commands.command(name="t", description="Get a tag list")
    async def tags(
        self,
        interaction: Interaction,
        search: str = "",
        category: Category | None = None,
    ) -> None:
        """Search and retrieve tags from the API with optional category filtering.

        :param Interaction interaction: Discord interaction object
        :param str search: Search term for tag names, defaults to ""
        :param Category | None category: Tag category to filter by, defaults to None
        """
        if interaction.channel is None or isinstance(
            interaction.channel,
            (discord.GroupChannel, discord.Thread),
        ):
            msg = f"You are not in channel {self._get_random_emoji(interaction.guild)}"
            await interaction.response.send_message(msg, ephemeral=True)
            return
        if (
            not isinstance(interaction.channel, discord.DMChannel)
            and not interaction.channel.nsfw
        ):
            emoji = self._get_random_emoji(interaction.guild)
            msg = f"You are not in an NSFW channel. {emoji}"
            await interaction.response.send_message(msg, ephemeral=True)
            return
        if interaction.user.id != self.bot.owner_id:
            await interaction.response.send_message(
                "Вы не в белом списке",
                ephemeral=True,
            )
            return

        await interaction.response.defer(thinking=True, ephemeral=True)
        try:
            res = await self.bot.api_client.get_tags(search, category)
            if len(str(res)) > MAX_MESSAGE_LENGTH:
                file_buffer = io.BytesIO(str(res).encode("utf-8"))
                await interaction.followup.send(
                    file=discord.File(file_buffer, "tags.txt"),
                )
            else:
                await interaction.followup.send(content=str(res))
        except Exception:
            logger.exception("Failed to get tags")
            await interaction.followup.send("Failed to get tags")


async def setup(bot: FGPBot) -> None:
    """Add the ApiCog to the bot.

    It relies on the bot's api_client

    :param FGPBot bot: The bot instance to add the cog to.
    """
    await bot.add_cog(ApiCog(bot))
