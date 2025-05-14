"""Database class for storing and retrieving file records."""

import logging
from pathlib import Path
from typing import Any

import aiosqlite

from core.models import FileRecord

logger = logging.getLogger("FileDatabase")


class FileDatabase:
    """Database class for storing and retrieving file records."""

    def __init__(self, db_path: Path) -> None:
        """Initialize the FileDatabase with the specified database path."""
        self.db_path = db_path

    async def connect(self) -> None:
        """Connect to the database and create tables if they don't exist."""
        self.conn = await aiosqlite.connect(self.db_path)
        self.conn.row_factory = aiosqlite.Row
        await self._create_tables()

    async def _create_tables(self) -> None:
        await self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_hash TEXT(64) NOT NULL UNIQUE,
                file_path TEXT NOT NULL,
                file_size INTEGER NOT NULL,
                converted_path TEXT,
                converted_hash TEXT(64),
                converted_size INTEGER,
                category TEXT NOT NULL,
                guild_usage TEXT DEFAULT "{}",
                created_at DATETIME NOT NULL
            )
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_file_hash ON file_tracking (file_hash)
        """)
        await self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_guild_usage
            ON file_tracking (json_extract(guild_usage, '$'))
        """)
        await self.conn.commit()

    async def insert_file_record(self, file_record: FileRecord) -> None:
        """Insert a new file record into the file_tracking table.

        This method takes a FileRecord object and inserts its data into the
        database. The fields inserted include guild ID, original file path,
         file hash, file size, converted file path, converted file
        hash, converted file size, send count, creation timestamp, and last
        sent timestamp.

        :param file_record: The FileRecord object containing the
            data to be inserted into the database.
        :type file_record: FileRecord
        """
        sql = """
            INSERT INTO file_tracking (
                file_hash, file_path, file_size, converted_path,
                converted_hash, converted_size, category, guild_usage, created_at
            )
            VALUES (:file_hash, :file_path, :file_size, :converted_path,
                :converted_hash, :converted_size, :category, :guild_usage, :created_at)
        """
        values = file_record.model_dump(mode="json")
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, values)
            await self.conn.commit()

    async def insert_file_records(self, file_records: list[FileRecord]) -> None:
        """Insert multiple file records into the file_tracking table.

        This method takes a list of FileRecord objects and inserts their data into the
        database in a single batch operation.
        The fields inserted include guild ID, original file path, filename, file hash,
        file size, converted file path, converted file hash,converted file size,
        send count, creation timestamp, and last sent timestamp.

        :param file_records: A list of FileRecord objects containing the data to be
                            inserted into the database.
        :type file_records: List[FileRecord]
        """
        sql = """
            INSERT INTO file_tracking (
                file_hash, file_path, file_size, converted_path,
                converted_hash, converted_size, category, guild_usage, created_at
            )
            VALUES (:file_hash, :file_path, :file_size, :converted_path,
                :converted_hash, :converted_size, :category, :guild_usage, :created_at)
        """

        data_to_insert: list[Any] = [
            rec.model_dump(mode="json") for rec in file_records
        ]
        async with self.conn.cursor() as cursor:
            await cursor.executemany(sql, data_to_insert)
            await self.conn.commit()

    async def _row_to_file_record(self, row: aiosqlite.Row) -> FileRecord:
        row_dict = dict(row)
        return FileRecord.model_validate(row_dict)

    async def get_file_record_by_hash(self, file_hash: str) -> FileRecord | None:
        """Get a file record from the database by file hash.

        :param file_hash: The file hash of the file record to retrieve.
        :type file_hash: str
        :return: The file record if found, otherwise None.
        :rtype: FileRecord | None
        """
        sql = """SELECT * FROM file_tracking WHERE file_hash = ?"""
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (file_hash,))
            row = await cursor.fetchone()
            if row is None:
                return None
            return await self._row_to_file_record(row)

    async def increment_send_count(
        self,
        file_hash: str,
        guild_id: str,
    ) -> FileRecord | None:
        """Increment the send count and update the last sent timestamp.

        :param file_hash: The file hash of the file record to update.
        :type file_hash: str
        :param guild_id: The guild ID associated with the file record.
        :type guild_id: str
        :return: The updated file record if successful, otherwise None.
        :rtype: FileRecord | None
        """
        sql = """
        UPDATE file_tracking SET
            guild_usage = json_set(
                guild_usage,
                '$.' || ? || '.send_count',
                COALESCE(json_extract(guild_usage, '$.' || ? || '.send_count'), 0) + 1,
                '$.' || ? || '.last_sent',
                CURRENT_TIMESTAMP
            )
        WHERE file_hash = ?
        RETURNING *
        """
        try:
            async with self.conn.cursor() as cursor:
                await cursor.execute(sql, (guild_id, guild_id, guild_id, file_hash))
                updated = await cursor.fetchone()
                if not updated:
                    await cursor.execute("ROLLBACK")
                    return None
                await self.conn.commit()
                return await self._row_to_file_record(updated)
        except aiosqlite.Error:
            logger.exception("Error incrementing send count.")
            await self.conn.rollback()
            return None

    async def update_converted_file(
        self,
        file_hash: str,
        converted_path: Path,
        converted_hash: str,
        converted_size: int,
    ) -> FileRecord | None:
        """Update the converted file path, hash, and size for a file record.

        :param file_hash: The file hash of the file record to update the conversion
        :type file_hash: str
        :param converted_path:
        :type converted_path: Path
        :param converted_hash: The converted file hash
        :type converted_hash: str
        :param converted_size: The converted file size
        :type converted_size: int
        :return: The updated file record if successful, otherwise None
        :rtype: FileRecord | None
        """
        sql = """
        UPDATE file_tracking SET
        converted_path = ?,
        converted_hash = ?,
        converted_size = ?
        WHERE file_hash = ?
        RETURNING *"""
        values = (str(converted_path), converted_hash, converted_size, file_hash)
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, values)
            updated = await cursor.fetchone()
            await self.conn.commit()
            if updated is None:
                return None
            return await self._row_to_file_record(updated)

    async def clear_conversion(self, file_hash: str) -> FileRecord | None:
        """Clear the conversion details for a file record.

        :param file_hash: The file hash of the file record to clear the conversion
        :type file_hash: str
        :return: The updated file record if successful, otherwise None
        :rtype: FileRecord | None
        """
        sql = """
        UPDATE file_tracking SET
        converted_path = NULL,
        converted_hash = NULL,
        converted_size = NULL
        WHERE file_hash = ?
        RETURNING *"""
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (file_hash,))
            updated = await cursor.fetchone()
            await self.conn.commit()
            if updated is None:
                return None
            return await self._row_to_file_record(updated)

    async def get_count_of_type(
        self,
        category: str,
    ) -> int:
        """Get the count of file records for a specific category.

        :param category: The category of the file records to count.
        :type category: str
        :return: The count of file records for the specified category.
        :rtype: int
        """
        sql = "SELECT COUNT(*) FROM file_tracking WHERE category = ?"
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (category,))
            row = await cursor.fetchone()
            if row is None:
                return 0
            return row[0]

    async def get_unsent_files(self, guild_id: str, category: str) -> list[FileRecord]:
        """Get all file records that have not been sent.

        :return: A list of file records that have not been sent.
        :rtype: list[FileRecord]
        """
        sql = """
        SELECT *
        FROM file_tracking
        WHERE category = ?
        AND (
            guild_usage IS NULL
            OR json_extract(guild_usage, '$.' || ?) IS NULL
            OR json_extract(guild_usage, '$.' || ? || '.send_count') = 0
        )
        """
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (category, guild_id, guild_id))
            rows = await cursor.fetchall()
            return [await self._row_to_file_record(row) for row in rows]

    async def get_files_larger_than(self, size: int) -> list[FileRecord]:
        """Get all file records that are larger than a specific size.

        :param size: The size in bytes to compare against
        :type size: int
        :return: A list of file records that are larger than the specified size.
        :rtype: list[FileRecord]
        """
        sql = "SELECT * FROM file_tracking WHERE file_size > ?"
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (size,))
            rows = await cursor.fetchall()
            return [await self._row_to_file_record(row) for row in rows]

    async def get_all_file_hashes(self) -> tuple[str, ...]:
        """Get all file hashes from the database.

        :return: A tuple of file hashes.
        :rtype: tuple[str, ...]
        """
        sql = "SELECT file_hash FROM file_tracking"
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql)
            rows = await cursor.fetchall()
            return tuple(row[0] for row in rows)

    async def get_all_filepaths_of_category(self, category: str) -> tuple[Path, ...]:
        """Get all file paths of a specific category.

        :param category: The category of the file records to retrieve.
        :type category: str
        :return: A tuple of file paths.
        :rtype: tuple[Path, ...]
        """
        sql = "SELECT file_path FROM file_tracking WHERE category = ?"
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, (category,))
            rows = await cursor.fetchall()
            return tuple(Path(row[0]) for row in rows)
