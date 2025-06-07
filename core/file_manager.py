"""File management system for synchronizing directory contents with a database.

This module provides functionality to track files in specified directories,
record their metadata in a database, and detect new or duplicate files using
hash-based comparisons. It's designed to handle asynchronous file operations
and maintain consistency between disk contents and database records.
"""

import logging
import random
import secrets
import shutil
from collections import defaultdict
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

import discord

from config import CATEGORY_MAP, CONVERTED_DIR, MAX_FILE_SIZE
from core.database import FileDatabase
from core.models import FileRecord
from utils import (
    file_exists,
    get_count_of_files,
    get_file_size,
    get_files,
    hash_file,
)
from utils.compress_utils import compress_image, compress_video

logger = logging.getLogger("FileManager")


class FileManager:
    """Manages synchronization between file directories and a database.

    This class handles the detection of new files, hash-based duplicate checking,
    and maintaining consistent records in the associated database. It operates
    on predefined directory categories specified in CATEGORY_MAP.

    Attributes:
        db: Database interface for storing and querying file records

    """

    def __init__(self, db: FileDatabase) -> None:
        """Initialize the FileManager with a database connection.

        :param FileDatabase db: Configured FileDatabase connection.
                  The manager will use this connection for all database operations.

        .. note::
            The FileManager does not manage the database connection lifecycle -
            the provided database should already be properly initialized.

        """
        self.db = db

    async def load_all_files(self) -> None:
        """Load new unique files from all configured directories into the database.

        Also removes files from the database that are no longer present on disk.

        Remain files if a converted version exists but the original is lost.
        """
        logger.info("Initiating file database synchronization")

        potential_records = await self._collect_potential_records()
        if not potential_records:
            logger.info("No new files found for processing")
            return

        unique_records = self._deduplicate_records(potential_records)
        existing_hashes = set(await self.db.get_all_file_hashes())
        final_records = [
            r for r in unique_records if r.file_hash not in existing_hashes
        ]

        await self._process_final_records(final_records)

    async def _collect_potential_records(self) -> list[FileRecord]:
        """Gather potential new file records from all configured directories.

        :return list[FileRecord]: List of potential new file records
        """
        return [
            record
            for directory, category in CATEGORY_MAP.items()
            async for record in self._process_directory(directory, category)
        ]

    async def _process_directory(
        self,
        directory: Path,
        category: str,
    ) -> AsyncGenerator[FileRecord, None]:
        """Process a directory recursively and yield new file records.

        if a file exists in the database but not on disk, it will be deleted from the db

        :param Path directory: Directory path to process
        :param str category: Category of the directory
        :return: New file records for files not in the database
        :rtype: AsyncGenerator[FileRecord, None]
        """
        logger.debug("Processing directory: %s (%s)", directory, category)

        existing_db_paths = set(await self.db.get_all_filepaths_of_category(category))
        disk_count = await get_count_of_files(directory)
        db_count = await self.db.get_count_of_type(category)

        if disk_count == db_count:
            logger.warning("Directory %s - file counts match", category)
        else:
            logger.debug("Mismatch detected: DB=%d, Disk=%d", db_count, disk_count)

        disk_paths: set[Path] = set()
        async for file_path in get_files(directory):
            disk_paths.add(file_path)
            if file_path in existing_db_paths:
                continue
            if file_path.name.endswith("_compressed"):
                logger.debug("Skipping compressed file: %s", file_path)
                continue

            logger.debug("New file detected: %s", file_path)
            yield await self._build_file_record(file_path, category)

        missing_paths = existing_db_paths - disk_paths
        for path in missing_paths:
            rec = await self.db.get_file_record_by_path(path)
            if await file_exists(path):
                continue  # double check
            if rec and rec.converted_path is not None:
                logger.warning(
                    "File in database not found on disk, but converted file exists: %s",
                    path,
                )
                continue
            logger.warning(
                "File in database not found on disk: %s (Category: %s)",
                path,
                category,
            )
            await self.db.delete_file_record_by_path(path)

    def _deduplicate_records(self, records: list[FileRecord]) -> list[FileRecord]:
        """Remove duplicate records based on file hash.

        :param list[FileRecord] records: List of file records to deduplicate
        :return list[FileRecord]: List of unique file records
        """
        hash_groups: dict[str, list[FileRecord]] = defaultdict(list)
        for record in records:
            hash_groups[record.file_hash].append(record)

        unique_records: list[FileRecord] = []
        for file_hash, group in hash_groups.items():
            if len(group) > 1:
                self._log_duplicates(file_hash, group)
            unique_records.append(group[0])

        return unique_records

    def _log_duplicates(self, file_hash: str, group: list[FileRecord]) -> None:
        """Log detailed information about duplicate files.

        :param str file_hash: Hash of the duplicate files
        :param list[FileRecord] group: List of duplicate file records
        """
        logger.warning("Duplicate files found with hash %s:", file_hash)
        for record in group:
            logger.warning("  - %s (Category: %s)", record.file_path, record.category)
        logger.info("Keeping first occurrence: %s", group[0].file_path)

    async def _process_final_records(self, records: list[FileRecord]) -> None:
        """Insert the final list of new unique file records into the database.

        :param list[FileRecord] records: List of file records to insert
        """
        if not records:
            logger.info("All potential files already exist in database")
            return

        logger.info("Inserting %d new unique file records", len(records))
        await self.db.insert_file_records(records)
        logger.debug("Successfully completed database update")

    async def _build_file_record(self, file_path: Path, category: str) -> FileRecord:
        """Create a FileRecord for the specified file path and category.

        :param Path file_path: Path to the file
        :param str category: Category of the file
        :return FileRecord: The created file record
        :raises Exception: If file processing fails
        """
        try:
            return FileRecord(
                file_path=file_path,
                file_hash=await hash_file(file_path),
                file_size=await get_file_size(file_path),
                category=category,
                created_at=datetime.now(UTC),
            )
        except Exception:
            logger.exception("Failed to process file %s.", file_path)
            raise

    async def compress_all_large_files(self) -> None:
        """Compress all media files larger than MAX_FILE_SIZE and update database."""
        max_size_bytes = MAX_FILE_SIZE

        large_files = await self.db.get_files_larger_than(max_size_bytes)
        logger.debug("Found %d large files to compress", len(large_files))
        logger.debug(
            "Target size: %d bytes (%.2f MB)",
            max_size_bytes,
            max_size_bytes / 1024 / 1024,
        )
        for file_record in large_files:
            await self.compress_file_into_db(file_record)

    async def delete_original_file(self, file_record: FileRecord) -> None:
        """Delete the original file if a converted version exists.

        :param FileRecord file_record: The file record to process
        """
        if not file_record.converted_path:
            logger.warning(
                "File %s does not have a converted file, skipping deletion.",
                file_record.file_path,
            )
            return
        self._try_unlink(file_record.file_path)

    async def download_file(
        self,
        attachment: discord.Attachment,
        category: str,
    ) -> FileRecord:
        """Download a Discord attachment and create a file record.

        :param discord.Attachment attachment: The attachment to download
        :param str category: The category for the file
        :return FileRecord: The file record for the downloaded file
        :raises ValueError: If the category is invalid
        """
        target_dir = None
        for dir_path, cat in CATEGORY_MAP.items():
            if cat == category:
                target_dir = dir_path
                break

        if target_dir is None:
            msg = f"Invalid category: {category}"
            raise ValueError(msg)

        filename = attachment.filename
        dest = target_dir / filename

        # Handle filename conflicts
        counter = 1
        while dest.exists():
            name_parts = filename.rsplit(".", 1)
            if not name_parts:
                name_parts = secrets.token_urlsafe(16)
            base = name_parts[0]
            ext = name_parts[1] if len(name_parts) > 1 else ""
            new_name = f"{base}_{counter}.{ext}" if ext else f"{base}_{counter}"
            dest = target_dir / new_name
            counter += 1

        logger.debug("Downloading %s to %s", attachment.filename, dest)
        await attachment.save(dest)

        return await self._build_file_record(dest, category)

    async def compress_file(
        self,
        record: FileRecord,
        target_size: int = MAX_FILE_SIZE,
    ) -> FileRecord | None:
        """Compress the file and return a new record for the compressed version.

        :param FileRecord record: The file record to compress
        :return: New record for the compressed file, or None if compression fails
        :rtype: FileRecord | None
        """
        try:
            suffix = record.file_path.suffix.lower()
            if suffix in [".jpg", ".jpeg", ".png", ".gif"]:
                compressed_path = await compress_image(record.file_path, target_size)
            elif suffix in [".mp4", ".avi", ".mov", ".mkv", ".webm"]:
                container = record.file_path.suffix[1:]
                compressed_path = await compress_video(
                    record.file_path,
                    target_size,
                    container,
                )
            else:
                logger.warning("Unsupported file type: %s", record.file_path)
                return None

            converted_dir = CONVERTED_DIR / record.category
            converted_dir.mkdir(parents=True, exist_ok=True)
            final_path = converted_dir / compressed_path.name

            i = 1
            while final_path.exists():
                stem = compressed_path.stem
                ext = compressed_path.suffix
                final_path = converted_dir / f"{stem}_{i}{ext}"
                i += 1

            shutil.move(str(compressed_path), str(final_path))

            return FileRecord(
                file_path=final_path,
                file_hash=await hash_file(final_path),
                file_size=await get_file_size(final_path),
                category=record.category,
                created_at=datetime.now(UTC),
            )

        except Exception:
            logger.exception("Compression failed.")
            return None

    async def fetch_unsent_file(
        self,
        guild_id: str,
        category: str,
    ) -> FileRecord | None:
        """Retrieve an unsent file for the guild and category under the size limit.

        :param str guild_id: The guild ID
        :param str category: The category of the file
        :return: The file record if found, otherwise None
        :rtype: FileRecord | None
        """
        max_attempts = 4

        records = await self.db.get_unsent_files(guild_id, category)
        if not records:
            return None
        random.shuffle(records)
        for record in records[:max_attempts]:
            if record.file_size <= MAX_FILE_SIZE:
                return record
            converted = await self.compress_file_into_db(record)
            if (
                converted
                and (converted.converted_size or converted.file_size) <= MAX_FILE_SIZE
            ):
                return converted
        logger.warning(
            "Failed to find a file under the size limit for guild %s and category %s",
            guild_id,
            category,
        )
        return None

    def _try_unlink(self, path: Path) -> None:
        """Attempt to delete the specified file path, logging any errors.

        :param Path path: The file path to delete
        """
        try:
            if path.exists():
                path.unlink()
        except Exception:
            logger.exception("Failed to unlink %s.", path)

    async def delete_file_record(self, file_record: FileRecord) -> None:
        """Remove the file record from the database and delete associated files.

        :param FileRecord file_record: The file record to delete
        """
        self._try_unlink(file_record.file_path)
        if file_record.converted_path:
            self._try_unlink(file_record.converted_path)
        await self.db.delete_file_record_by_hash(file_record.file_hash)

    async def increment_send_count(self, file_hash: str, guild_id: str) -> None:
        """Increase the send count for the file and guild, updating the last sent time.

        :param str file_hash: The file hash of the file record to update.
        :param str guild_id: The guild ID associated with the file record.
        """
        await self.db.increment_send_count(file_hash, guild_id)

    async def check_file_size(self, file_path: Path) -> tuple[int, bool]:
        """Determine the file size and if it is within the allowed size limit.

        :param Path file_path: The path to the file.
        :return: File size and if it's under the limit
        :rtype: tuple[int, bool]
        """
        file_size = await get_file_size(file_path)
        return (file_size, file_size <= MAX_FILE_SIZE)

    async def add_file_to_db(self, file_record: FileRecord) -> FileRecord | None:
        """Insert a new file record into the database after validation.

        :param FileRecord file_record: The file record to add
        :return: The added file record, or None if addition fails
        :rtype: FileRecord | None
        """
        # Validate category
        category = file_record.category
        if category not in CATEGORY_MAP.values():
            logger.error("Invalid category: %s", category)
            return None

        file_path = file_record.file_path
        if not file_path.exists():
            logger.error("File does not exist: %s", file_path)
            return None

        converted_path = file_record.converted_path
        if converted_path and not converted_path.exists():
            logger.error("Converted file does not exist: %s", converted_path)
            return None

        existing_record = await self.db.get_file_record_by_hash(file_record.file_hash)
        if existing_record:
            logger.info("File already exists in database: %s", file_path)
            if existing_record.file_path != file_path:
                self._try_unlink(file_path)
            return existing_record

        # Insert into database
        await self.db.insert_file_record(file_record)
        logger.info("Added new file to database: %s", file_path)
        return file_record

    async def compress_file_into_db(self, file_record: FileRecord) -> None | FileRecord:
        """Compress the file if needed and update its database record.

        :param FileRecord file_record: The file record to compress
        :return: Updated file record, or None if compression fails
        :rtype: FileRecord | None
        """
        if file_record.file_size < MAX_FILE_SIZE:
            logger.info(
                "File %s is under the size limit, no need to compress.",
                file_record.file_path,
            )
            return file_record

        # Skip already compressed files
        if file_record.converted_path:
            logger.info("File already compressed: %s", file_record.file_path)
            return file_record

        compressed_record = await self.compress_file(file_record)
        if not compressed_record:
            return None

        res = await self.db.update_converted_file(
            file_record.file_hash,
            compressed_record.file_path,
            compressed_record.file_hash,
            compressed_record.file_size,
        )
        if not res:
            logger.error(
                "Failed to update database with compressed file: %s",
                file_record.file_path,
            )
            return None
        logger.info(
            "Updated database with compressed file: %s",
            compressed_record.file_path,
        )
        return res

    async def find_file(self, file_id: str, category: str) -> FileRecord | None:
        """Find a file by its identifier and category.

        :param str file_id: The file identifier (hash or filename)
        :param str category: The category of the file
        :return: The file record if found, otherwise None
        :rtype: FileRecord | None
        """
        record = await self.db.get_file_record_by_hash(file_id)
        if record:
            if record.category != category:
                logger.warning(
                    "Found a file with the same hash but different category: %s",
                    file_id,
                )
                return None
            return record
        records = await self.db.get_file_records_by_filename(file_id)
        if records:
            if len(records) > 1:
                logger.warning(
                    "Found multiple files with the same filename: %s",
                    file_id,
                )
                return None
            if records[0].category != category:
                logger.warning(
                    "Found a file with the same filename but different category: %s",
                    file_id,
                )
                return None
            return record
        return None

    async def get_file_record_by_hash(self, file_hash: str) -> FileRecord | None:
        """Retrieve a file record by its hash.

        :param str file_hash: The hash of the file
        :return: The file record if found, otherwise None
        :rtype: FileRecord | None
        """
        return await self.db.get_file_record_by_hash(file_hash)
