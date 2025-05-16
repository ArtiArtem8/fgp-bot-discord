"""File management system for synchronizing directory contents with a database.

This module provides functionality to track files in specified directories,
record their metadata in a database, and detect new or duplicate files using
hash-based comparisons. It's designed to handle asynchronous file operations
and maintain consistency between disk contents and database records.
"""

import logging
import random
import shutil
from collections import defaultdict
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

from config import CONVERTED_DIR, MEME_DIR, PRIVATE_DIR
from core.database import FileDatabase
from core.models import FileRecord
from utils import get_count_of_files, get_file_size, get_files, hash_file
from utils.compress_utils import compress_image, compress_video

logger = logging.getLogger("FileManager")

CATEGORY_MAP: dict[Path, str] = {
    MEME_DIR: "meme",
    PRIVATE_DIR: "private",
}
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 mega bytes


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
        """Entry point for loading files into the database."""
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
        """Collect potential new file records from all directories."""
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
        """Process a single directory and yield new file records."""
        logger.debug("Processing directory: %s (%s)", directory, category)

        existing_paths = set(await self.db.get_all_filepaths_of_category(category))
        disk_count = await get_count_of_files(directory)
        db_count = await self.db.get_count_of_type(category)

        if disk_count == db_count:
            logger.debug("Skipping %s - file counts match", category)
            return

        logger.debug("Mismatch detected: DB=%d, Disk=%d", db_count, disk_count)
        disk_paths: set[Path] = set()
        async for file_path in get_files(directory):
            disk_paths.add(file_path)
            if file_path in existing_paths:
                continue
            if file_path.name.endswith("_compressed"):
                logger.debug("Skipping compressed file: %s", file_path)
                continue

            logger.debug("New file detected: %s", file_path)
            yield await self._build_file_record(file_path, category)
        missing_paths = existing_paths - disk_paths
        for path in missing_paths:
            logger.warning(
                "File in database not found on disk: %s (Category: %s)",
                path,
                category,
            )
            await self.db.delete_file_record_by_path(path)

    def _deduplicate_records(self, records: list[FileRecord]) -> list[FileRecord]:
        """Remove duplicate records based on file hash."""
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
        """Log detailed information about duplicate files."""
        logger.warning("Duplicate files found with hash %s:", file_hash)
        for record in group:
            logger.warning("  - %s (Category: %s)", record.file_path, record.category)
        logger.info("Keeping first occurrence: %s", group[0].file_path)

    async def _process_final_records(self, records: list[FileRecord]) -> None:
        """Process and insert validated records into the database."""
        if not records:
            logger.info("All potential files already exist in database")
            return

        logger.info("Inserting %d new unique file records", len(records))
        await self.db.insert_file_records(records)
        logger.debug("Successfully completed database update")

    async def _build_file_record(self, file_path: Path, category: str) -> FileRecord:
        """Build a FileRecord with proper error handling."""
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
        """Compress media files larger than MAX_FILE_SIZE and update the database."""
        max_size_bytes = MAX_FILE_SIZE

        large_files = await self.db.get_files_larger_than(max_size_bytes)
        logger.debug("Found %d large files to compress", len(large_files))
        logger.debug(
            "Target size: %d bytes (%.2f MB)",
            max_size_bytes,
            max_size_bytes / 1024 / 1024,
        )
        for file_record in large_files:
            await self._compress_and_update(file_record)

    async def _compress_and_update(self, file_record: FileRecord) -> None | FileRecord:
        """Compress and update the file record if necessary.

        :param FileRecord file_record: The file record to process.
        """
        if file_record.converted_path is not None:
            logger.info(
                "File already has a converted version: %s",
                file_record.file_path,
            )
            return None

        max_size_bytes = MAX_FILE_SIZE
        if file_record.file_size <= max_size_bytes:
            logger.info(
                "File %s is under the size limit, no need to compress.",
                file_record.file_path,
            )
            return None

        try:
            # Determine compression method based on file type
            suffix = file_record.file_path.suffix.lower()
            if suffix in [".jpg", ".jpeg", ".png", ".gif"]:
                compressed_path = await compress_image(
                    file_record.file_path,
                    max_size_bytes,
                )
            elif suffix in [".mp4", ".avi", ".mov", ".mkv"]:
                logger.debug("Compressing video: %s", file_record.file_path)
                compressed_path = await compress_video(
                    file_record.file_path,
                    max_size_bytes,
                )
            else:
                logger.warning(
                    "Unsupported file type for compression: %s",
                    file_record.file_path,
                )
                return None

            category_dir = CONVERTED_DIR / file_record.category
            category_dir.mkdir(parents=True, exist_ok=True)
            final_compressed_path = category_dir / compressed_path.name

            i = 1
            while final_compressed_path.exists():
                stem = compressed_path.stem
                suffix = compressed_path.suffix
                final_compressed_path = category_dir / f"{stem}_{i}{suffix}"
                i += 1

            shutil.move(str(compressed_path), str(final_compressed_path))

            # Update database
            compressed_hash = await hash_file(final_compressed_path)
            compressed_size = await get_file_size(final_compressed_path)

            res = await self.db.update_converted_file(
                file_record.file_hash,
                final_compressed_path,
                compressed_hash,
                compressed_size,
            )
            if res is None:
                logger.error(
                    "Failed to update database for file: %s",
                    file_record.file_path,
                )
                return None
        except Exception:
            logger.exception("Failed to compress %s.", file_record.file_path)
            raise
        else:
            logger.info(
                "Compressed %s to %s",
                file_record.file_path,
                final_compressed_path,
            )
            return res

    async def compress_single_file(self, file_path: Path) -> None:
        """Compress a single file if it exceeds the size limit and update the database.

        Args:
            file_path: Path to the file to compress.

        """
        raise NotImplementedError
        file_record = await self.db.get_file_record_by_hash(file_path)
        if not file_record:
            logger.error("File %s not found in the database.", file_path)
            return

        await self._compress_and_update(file_record)

    async def fetch_unsent_file(
        self,
        guild_id: str,
        category: str,
    ) -> FileRecord | None:
        """Fetch files that have not yet been sent and are under the size limit."""
        max_attempts = 4

        records = await self.db.get_unsent_files(guild_id, category)
        if not records:
            return None
        random.shuffle(records)
        for record in records[:max_attempts]:
            if record.file_size <= MAX_FILE_SIZE:
                return record
            converted = await self._compress_and_update(record)
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
