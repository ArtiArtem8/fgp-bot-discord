"""File management system for synchronizing directory contents with a database.

This module provides functionality to track files in specified directories,
record their metadata in a database, and detect new or duplicate files using
hash-based comparisons. It's designed to handle asynchronous file operations
and maintain consistency between disk contents and database records.
"""

import logging
from collections import defaultdict
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

from config import MEME_DIR, PRIVATE_DIR
from core.database import FileDatabase
from core.models import FileRecord
from utils import get_count_of_files, get_file_size, get_files, hash_file

logger = logging.getLogger("FileManager")

CATEGORY_MAP: dict[Path, str] = {
    MEME_DIR: "meme",
    PRIVATE_DIR: "private",
}


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

        :param db: Configured FileDatabase connection.
                  The manager will use this connection for all database operations.
        :type db: FileDatabase

        .. note::
            The FileManager does not manage the database connection lifecycle -
            the provided database should already be properly initialized.

        """
        self.db = db

    async def _load_all_files(self) -> None:
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

        async for file_path in get_files(directory):
            if file_path in existing_paths:
                continue

            logger.debug("New file detected: %s", file_path)
            yield await self._build_file_record(file_path, category)

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
