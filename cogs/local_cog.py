"""Cog for handling local file storage and retrieval operations in Discord.

This module provides commands for sharing memes and private content,
enforcing NSFW checks, and providing server-specific file size limits.
"""

import datetime
import logging
import secrets
from pathlib import Path

import discord
from discord import Interaction, WebhookMessage, app_commands, ui
from discord.ext import commands
from discord.utils import format_dt

from config import CATEGORY_MAP, MAX_FILE_SIZE, MEME_DIR, PRIVATE_DIR
from core.models import FileRecord
from main import FGPBot

logger = logging.getLogger("LocalCog")


MEME = CATEGORY_MAP.get(MEME_DIR, "meme")
PRIVATE = CATEGORY_MAP.get(PRIVATE_DIR, "private")


class LocalCog(commands.Cog):
    """Cog for managing and serving locally stored content in Discord."""

    def __init__(self, bot: FGPBot) -> None:
        """Initialize the LocalCog with bot instance and dependencies.

        :param `FGPBot` bot: The bot instance.
        """
        self.bot = bot

        logger.debug("Cog initialized")

    @staticmethod
    def _get_guild_id(interaction: Interaction) -> str:
        """Generate a guild identifier string from interaction context.

        :param `Interaction` interaction: The interaction context
        :return str: The guild identifier or user-specific identifier if no guild.
        """
        return (
            str(interaction.guild_id)
            if interaction.guild_id
            else f"user_{interaction.user.id}"
        )

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

    async def edit(
        self,
        message: WebhookMessage,
        *,
        content: str | None = None,
        view: ui.View | None = None,
    ) -> WebhookMessage:
        """Edits the message and applies a random static emoji.

        :param WebhookMessage message: The message to edit.
        :param str | None content: The new content to set, if provided.
        :param ui.View | None view: The new view to set, if provided.
        :return WebhookMessage: The edited message.
        """
        emoji: discord.Emoji = self._get_random_emoji(message.guild)
        if content:
            content = content + " " + str(emoji)
        return await message.edit(content=content, view=view)

    def is_file_record_within_size_limit(self, file: FileRecord) -> bool:
        """Check if a file record is within the size limit.

        :param FileRecord file: The file record to check.
        :return bool: True if the file size is within the limit, False otherwise.
        """
        if file.converted_size:
            return file.converted_size <= MAX_FILE_SIZE
        return file.file_size <= MAX_FILE_SIZE

    async def _get_random_file(
        self,
        message: WebhookMessage,
        guild_id: str,
        content_type: str,
    ) -> tuple[FileRecord, Path] | None:
        """Fetch a random unsent file for the given guild and content type.

        If successful, return the FileRecord and its path;
        otherwise, update the message with an appropriate error
        and return None.

        :param WebhookMessage message: The message to update with status or errors.
        :param str guild_id: The guild identifier.
        :param str content_type: The type of content (e.g., 'meme' or 'private').
        :return: The file record and its path if found, None otherwise.
        :rtype: tuple[FileRecord, Path] | None
        """
        try:
            file = await self.bot.file_manager.fetch_unsent_file(
                guild_id,
                content_type,
            )
        except Exception:
            logger.exception("Failed to fetch %s file", content_type)
            msg = f"Files not available for {content_type}"
            await self.edit(message, content=msg, view=None)
            return None
        if not file:
            msg = f"No {content_type} available"
            await self.edit(message, content=msg, view=None)
            return None
        return (file, self._try_get_converted_file_path(file))

    async def _get_file_by_identifier(
        self,
        message: WebhookMessage,
        identifier: str,
        content_type: str,
    ) -> tuple[FileRecord, Path] | None:
        """Retrieve the FileRecord and Path for the file.

        If the file is not found, update the message and return None.

        :param WebhookMessage message: The message to update with status or errors.
        :param str identifier: The file identifier (e.g., hash).
        :param str content_type: The type of content (e.g., 'meme' or 'private').
        :return: The file record and its path if found, None otherwise.
        :rtype: tuple[FileRecord, Path] | None
        """
        file = await self.bot.file_manager.find_file(identifier, content_type)
        if not file:
            await self.edit(message, content="Invalid file identifier", view=None)
            return None
        return (file, self._try_get_converted_file_path(file))

    def _try_get_converted_file_path(self, file: FileRecord) -> Path:
        """Determine the appropriate file path to use.

        Preferring the converted path if it exists and is within size limits.

        :param FileRecord file: The file record to evaluate.
        :return Path: The selected file path (converted or original).
        """
        if (
            file.converted_size
            and file.converted_hash
            and file.converted_path
            and file.file_size > MAX_FILE_SIZE
            and file.converted_size <= MAX_FILE_SIZE
        ):
            return file.converted_path
        return file.file_path

    async def send_file(
        self,
        interaction: Interaction,
        message: WebhookMessage,
        file_path: Path | None,
        file_name: str | None = None,
    ) -> bool:
        """Send the file to channel, updating the message with sending status.

        :param Interaction interaction: The interaction context.
        :param WebhookMessage message: The message to update with status.
        :param Path | None file_path: The path to the file to send.
        :param str | None file_name: The name to use for the file, if provided.
        :return bool: True if the file was sent successfully, False otherwise.
        """
        if not file_path:
            await self.edit(message, content="No files available", view=None)
            return False
        try:
            await self.edit(
                message,
                content="Sending file...",
                view=None,
            )
            await interaction.channel.send(  # type: ignore[attr-defined]
                file=discord.File(file_path, file_name),
            )
            await self.edit(
                message,
                content="File sent!",
                view=None,
            )
        except Exception:
            msg = "Failed to send file"
            logger.exception(msg)
            await self.edit(message, content=msg)
            return False
        else:
            return True

    async def send_file_by_identifier(
        self,
        interaction: Interaction,
        message: WebhookMessage,
        identifier: str,
        content_type: str,
    ) -> None:
        """Send a file by its identifier, with size limit checks and user confirmation.

        :param Interaction interaction: The interaction context.
        :param WebhookMessage message: The message to update with status.
        :param str identifier: The file identifier (e.g., hash, filename).
        :param str content_type: The type of content (e.g., 'meme' or 'private').
        """
        file = await self._get_file_by_identifier(message, identifier, content_type)
        if not file:
            return
        file_path = file[1]
        file_hash = file[0].file_hash
        await self.edit(message, content="File Found!")
        if not self.is_file_record_within_size_limit(file[0]):
            if file_path == file[0].file_path:
                file_size = file[0].file_size
            else:
                file_size = file[0].converted_size or 16**6
                file_hash = file[0].converted_hash or file_hash
            size_str = self.human_readable_size(file_size)
            msg = f"File is too large to send ({size_str})"
            view = SendingConfirmationView(15)
            await self.edit(message, content=msg, view=view)
            await view.wait()
            if view.choice is None:
                msg = "⏰ Confirmation timed out"
                await self.edit(message, content=msg, view=None)
                return
            if not view.choice:
                msg = "Cancelled"
                await self.edit(message, content=msg, view=None)
                return
        suffix = file_path.suffix.lower()
        file_name = f"{file_hash}{suffix}"
        await self.send_file(interaction, message, file_path, file_name)

    async def send_random_file(
        self,
        interaction: Interaction,
        message: WebhookMessage,
        guild_id: str,
        content_type: str,
        *,
        increment_send_count: bool = True,
    ) -> None:
        """Send a random file for the guild and content type.

        Optionally incrementing the send count upon success.

        :param Interaction interaction: The interaction context.
        :param WebhookMessage message: The message to update with status.
        :param str guild_id: The guild identifier.
        :param str content_type: The type of content (e.g., 'meme' or 'private').
        :param increment_send_count: Whether to increment the send count, def to True.
        :type increment_send_count: bool
        """
        file = await self._get_random_file(message, guild_id, content_type)
        if not file:
            return
        file_path = file[1]
        if file_path == file[0].file_path:
            file_hash = file[0].file_hash
        else:
            file_hash = file[0].converted_hash or file[0].file_hash
        # file should already be within limits
        suffix = file_path.suffix.lower()
        file_name = f"{file_hash}{suffix}"
        res = await self.send_file(interaction, message, file_path, file_name)
        if increment_send_count and res:
            await self.bot.file_manager.increment_send_count(
                file[0].file_hash,
                guild_id,
            )

    @app_commands.command(name="m", description="Send a meme")
    async def meme(self, interaction: Interaction, *, identifier: str = "") -> None:
        """Send a meme to the channel, either a specific one by identifier or a random.

        A random meme will be unique to the guild

        :param Interaction interaction: The interaction context.
        :param str identifier: The file identifier, if provided.
        """
        if interaction.channel is None:
            await interaction.response.send_message(
                "No channel available",
                ephemeral=True,
            )
            return None

        await interaction.response.defer(thinking=True, ephemeral=True)
        message = await interaction.followup.send(
            f"Searching for meme... {self._get_random_emoji(interaction.guild)}",
            wait=True,
            silent=True,
            ephemeral=True,
        )
        if identifier:
            return await self.send_file_by_identifier(
                interaction,
                message,
                identifier,
                MEME,
            )

        guild_id = self._get_guild_id(interaction)
        return await self.send_random_file(
            interaction,
            message,
            guild_id,
            MEME,
            increment_send_count=True,
        )

    @app_commands.command(name="f", description="Send a NSFW")
    async def private(self, interaction: Interaction, *, identifier: str = "") -> None:
        """Send a private file, either by identifier or a random one, with NSFW checks.

        A random private will be unique to the guild

        :param Interaction interaction: The interaction context.
        :param str identifier: The file identifier, if provided.
        """
        if interaction.channel is None or isinstance(
            interaction.channel,
            (discord.GroupChannel, discord.Thread),
        ):
            msg = f"You are not in channel {self._get_random_emoji(interaction.guild)}"
            await interaction.response.send_message(msg, ephemeral=True)
            return None
        if (
            not isinstance(interaction.channel, discord.DMChannel)
            and not interaction.channel.nsfw
        ):
            emoji = self._get_random_emoji(interaction.guild)
            msg = f"You are not in an NSFW channel. {emoji}"
            await interaction.response.send_message(msg, ephemeral=True)
            return None
        await interaction.response.defer(thinking=True, ephemeral=True)
        message = await interaction.followup.send(
            f"Searching for private... {self._get_random_emoji(interaction.guild)}",
            wait=True,
            silent=True,
            ephemeral=True,
        )
        if identifier:
            return await self.send_file_by_identifier(
                interaction,
                message,
                identifier,
                PRIVATE,
            )

        guild_id = self._get_guild_id(interaction)
        return await self.send_random_file(
            interaction,
            message,
            guild_id,
            PRIVATE,
            increment_send_count=True,
        )

    @staticmethod
    def human_readable_size(size: float) -> str:
        """Convert a file size in bytes to a human-readable string (e.g., '1.23 MB').

        Units are starting from B (byte) and up to EB (exabyte).

        :param float size: The file size in bytes.
        :return str: The human-readable size string.
        """
        unit_size = 1024.0
        for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
            if size < unit_size:
                return f"{size:.2f} {unit}"
            size /= unit_size
        return f"{size:.2f} EB"

    async def add_file_flow(
        self,
        interaction: Interaction,
        message: WebhookMessage,
        file_record: FileRecord,
    ) -> None:
        """Manage file addition, including size checks and compression confirmation.

        :param Interaction interaction: The interaction context.
        :param WebhookMessage message: The message to update with status.
        :param FileRecord file_record: The file record to add.
        """
        if file_record.file_size > MAX_FILE_SIZE:
            timeout = 60
            view = CompressionConfirmationView(file_record, timeout)
            expires_at = format_dt(
                discord.utils.utcnow() + datetime.timedelta(seconds=timeout),
                "R",
            )

            size = self.human_readable_size(file_record.file_size)
            msg = (
                f"⚠️ File size ({size}) exceeds the limit!\n"
                f"Would you like to compress before adding?\n"
                f"This dialog expires {expires_at}"
            )

            message = await self.edit(message, content=msg, view=view)
            await view.wait()

            if view.choice is None:
                await self.edit(
                    message,
                    content="⏰ Confirmation timed out",
                    view=None,
                )
                await self.bot.file_manager.delete_file_record(file_record)
                return
            if not view.choice:
                await self.bot.file_manager.delete_file_record(file_record)
                return

            # Perform compression
            message = await self.edit(
                message,
                content="🔄 Compressing file... ",
                view=None,
            )
            try:
                compressed_record = await self.bot.file_manager.compress_file(
                    file_record,
                )
                if compressed_record is None:
                    await self.edit(message, content="❌ Compression failed")
                    await self.bot.file_manager.delete_file_record(file_record)
                    return
                compressed_record = FileRecord(
                    file_path=file_record.file_path,
                    file_hash=file_record.file_hash,
                    file_size=file_record.file_size,
                    category=file_record.category,
                    created_at=file_record.created_at,
                    converted_path=compressed_record.file_path,
                    converted_hash=compressed_record.file_hash,
                    converted_size=compressed_record.file_size,
                )
                await self.bot.file_manager.delete_original_file(file_record)
            except Exception as e:
                logger.exception("Failed to compress file")
                await self.edit(message, content=f"❌ Compression failed: {e!s}")
                await self.bot.file_manager.delete_file_record(file_record)
                return
            else:
                file_record = compressed_record or file_record

        try:
            final_record = await self.bot.file_manager.add_file_to_db(file_record)
            if not final_record:
                await self.edit(message, content="❌ Failed to add file")
                return
            success_msg = "✅ Successfully added file!\n" + self._record_display(
                final_record,
            )
            await self.edit(message, content=success_msg)

        except Exception as e:
            logger.exception("Failed to add file")
            await interaction.followup.send(f"❌ Failed to add file: {e!s}")
            await self.bot.file_manager.delete_file_record(file_record)

    def _record_display(self, file_record: FileRecord) -> str:
        """Generate a formatted string displaying the file record.

        :param FileRecord file_record: The file record to display.
        :return str: The formatted display string.
        """
        msg = (
            f"- Size: {self.human_readable_size(file_record.file_size)}\n"
            f"- Hash: `{file_record.file_hash}`\n"
            f"- Category: {file_record.category}\n"
        )
        if file_record.converted_size:
            size = self.human_readable_size(file_record.converted_size)
            msg += (
                f"- Compressed size: {size}\n"
                f"- Compressed hash: `{file_record.converted_hash}`\n"
            )
        return msg

    @app_commands.command(name="a", description="Add a file")
    async def add_file(
        self,
        interaction: Interaction,
        *,
        file: discord.Attachment,
        category: str,
    ) -> None:
        """Add a new file to the database, handling downloads, duplicate, and compression.

        :param Interaction interaction: The interaction context.
        :param discord.Attachment file: The file attachment to add.
        :param str category: The category for the file (e.g., 'meme' or 'private').
        """  # noqa: E501
        await interaction.response.defer(thinking=True)
        file_record = None
        message = None
        try:
            message = await interaction.followup.send(
                "⬇️ Downloading file...",
                wait=True,
            )
            file_record = await self.bot.file_manager.download_file(file, category)

            db_rec = await self.bot.file_manager.get_file_record_by_hash(
                file_record.file_hash,
            )
            if db_rec:
                await interaction.followup.send(
                    "⚠️ File already exists in database!\n"
                    + self._record_display(db_rec),
                )
                await self.bot.file_manager.delete_file_record(file_record)
                return None

            message = await message.edit(
                content=f"✅ Downloaded {file.filename}\n"
                f"Size: {self.human_readable_size(file_record.file_size)}",
            )

            await self.add_file_flow(interaction, message, file_record)

        except ValueError:
            fail_msg = f"❌ Invalid category {category}"
            if not message:
                return await interaction.followup.send(fail_msg)
            await message.edit(content=fail_msg)
        except Exception as e:
            logger.exception("File add failed")
            fail_msg = f"❌ Failed to add file: {e!s}"
            if not message:
                return await interaction.followup.send(fail_msg)
            await message.edit(content=fail_msg)
            if file_record is not None:
                await self.bot.file_manager.delete_file_record(file_record)


class CompressionConfirmationView(ui.View):
    """A view for confirming whether to compress a file that exceeds the size limit."""

    def __init__(self, file_record: FileRecord, timeout: float) -> None:
        """Initialize the CompressionConfirmationView with a file record and timeout."""
        super().__init__(timeout=timeout)
        self.file_record = file_record
        self.choice: bool | None = None

    @ui.button(label="Compress", style=discord.ButtonStyle.green)
    async def compress(  # noqa: D102
        self,
        interaction: Interaction,  # noqa: ARG002
        button: ui.Button["CompressionConfirmationView"],  # noqa: ARG002
    ) -> None:
        self.choice = True
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(  # noqa: D102
        self,
        interaction: Interaction,
        button: ui.Button["CompressionConfirmationView"],  # noqa: ARG002
    ) -> None:
        await interaction.response.edit_message(
            content=f"{interaction.user.mention} Operation cancelled",
            view=None,
        )
        self.choice = False
        self.stop()

    async def on_timeout(self) -> None:  # noqa: D102
        self.choice = None


class SendingConfirmationView(ui.View):
    """A view for confirming whether to attempt sending a big file."""

    def __init__(self, timeout: float) -> None:
        """Initialize the SendingConfirmationView with a timeout."""
        super().__init__(timeout=timeout)
        self.choice: bool | None = None

    @ui.button(label="Try to send", style=discord.ButtonStyle.green)
    async def send(  # noqa: D102
        self,
        interaction: Interaction,  # noqa: ARG002
        button: ui.Button["SendingConfirmationView"],  # noqa: ARG002
    ) -> None:
        self.choice = True
        self.stop()

    @ui.button(label="Cancel", style=discord.ButtonStyle.red)
    async def cancel(  # noqa: D102
        self,
        interaction: Interaction,  # noqa: ARG002
        button: ui.Button["SendingConfirmationView"],  # noqa: ARG002
    ) -> None:
        self.choice = False
        self.stop()

    async def on_timeout(self) -> None:  # noqa: D102
        self.choice = None


async def setup(bot: FGPBot) -> None:
    """Add the LocalCog to the bot.

    :param FGPBot bot: The bot instance to add the cog to.
    """
    await bot.add_cog(LocalCog(bot))
