import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import pytest

from core.database import FileDatabase, FileRecord


class TestDatabase:
    async def test_create_tables(self, database: FileDatabase):
        # Verify table creation
        async with database.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        ) as cursor:
            tables = await cursor.fetchall()
        assert "file_tracking" in [t[0] for t in tables]

    async def test_insert_and_retrieve_file(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        # Create test record
        record = sample_file_record
        # Insert and verify
        await database.insert_file_record(record)
        # Retrieve by hash
        retrieved = await database.get_file_record_by_hash(
            record.file_hash,
        )
        assert retrieved is not None
        assert retrieved.file_path.name == "doge.png"
        assert retrieved.guild_usage == {}

    async def test_duplicate_hash_insertion(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        record = sample_file_record
        await database.insert_file_record(record)

        with pytest.raises(Exception) as exc_info:
            await database.insert_file_record(record)

        assert "UNIQUE constraint failed" in str(exc_info.value)

    async def test_increment_send_count(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        # Insert test record
        record = sample_file_record
        await database.insert_file_record(record)
        guild_id = "1"
        # First increment
        updated = await database.increment_send_count(record.file_hash, guild_id)
        assert updated is not None
        guild_usage = updated.guild_usage[guild_id]
        assert guild_usage.send_count == 1
        assert guild_usage.last_sent is not None

        # Second increment
        updated = await database.increment_send_count(record.file_hash, guild_id)
        assert updated is not None
        assert updated.guild_usage[guild_id].send_count == 2

        # Third increment
        updated = await database.increment_send_count(record.file_hash, guild_id)
        assert updated is not None
        assert updated.guild_usage[guild_id].send_count == 3

    async def test_update_converted_file(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        record = sample_file_record
        await database.insert_file_record(record)

        converted_path = Path("converted/doge.webp")
        converted_size = 8_000_000
        converted_hash = "hash"

        updated = await database.update_converted_file(
            record.file_hash,
            converted_path,
            converted_hash,
            converted_size,
        )
        assert updated is not None
        assert updated.converted_path == converted_path
        assert updated.converted_size == converted_size


    async def test_get_nonexistent_file(self, database: FileDatabase) -> None:
        """Test retrieval of non-existent hash."""
        result = await database.get_file_record_by_hash("nonexistent_hash")
        assert result is None

    async def test_increment_nonexistent_file(self, database: FileDatabase) -> None:
        """Test incrementing count for non-existent file."""
        result = await database.increment_send_count("nonexistent_hash", "1")
        assert result is None

    async def test_absolute_path_conversion(self, database: FileDatabase):
        # Test that absolute paths are stored as relative
        abs_path = Path("/absolute/path/to/file.png")
        record = FileRecord(
            id=0,
            file_path=abs_path / "file.png",
            file_hash="a" * 64,
            file_size=100,
            created_at=datetime.now(),
        )
        await database.insert_file_record(record)
        retrieved = await database.get_file_record_by_hash("a" * 64)
        assert retrieved is not None
        assert not Path(retrieved.file_path).is_absolute()

    async def test_clear_conversion(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        # Test removing conversion details
        record = sample_file_record
        await database.insert_file_record(record)

        # First set conversion
        await database.update_converted_file(
            record.file_hash,
            Path("converted/file.webp"),
            "new_hash",
            8000,
        )

        # Clear conversion
        updated = await database.clear_conversion(record.file_hash)
        assert updated is not None
        assert updated.converted_path is None
        assert updated.converted_size is None

    async def test_concurrent_updates(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        # Test race condition handling
        record: FileRecord = sample_file_record
        guild_id = "1"
        await database.insert_file_record(record)
        async def _increment_counter():
            updated = await database.increment_send_count(record.file_hash, guild_id)
            assert updated is not None
            return 

        # Simulate concurrent increments
        _results = await asyncio.gather(
            _increment_counter(),
            _increment_counter(),
            _increment_counter(),
        )

        final = await database.get_file_record_by_hash(record.file_hash)
        assert final is not None
        assert final.guild_usage[guild_id].send_count == 3

    async def test_timezone_handling(self, database: FileDatabase):
        # Test TZ-aware datetime storage
        tz_aware_dt = datetime.now(UTC)
        record = FileRecord(
            id=0,
            file_path=Path("timezone_test") / "test.png",
            file_hash="b" * 64,
            file_size=100,
            created_at=tz_aware_dt,
        )
        await database.insert_file_record(record)
        retrieved = await database.get_file_record_by_hash("b" * 64)
        assert retrieved is not None
        assert retrieved.created_at.tzinfo is not None

    async def test_missing_required_field(self, database: FileDatabase):
        # Test database-level constraints
        with pytest.raises(sqlite3.IntegrityError):
            await database.conn.execute(
                "INSERT INTO file_tracking (file_hash) VALUES (?)",
                ("c" * 64,),
            )

    async def test_transaction_rollback(self, tmp_path: Path):
        db_path = tmp_path / "rollback_test.db"
        record = FileRecord(
            file_path=Path("rollback_test") / "test.png",
            file_hash="d" * 64,
            file_size=100,
            created_at=datetime.now(),
        )

        async with aiosqlite.connect(db_path) as conn:
            db = FileDatabase(db_path)
            db.conn = conn
            db.conn.row_factory = aiosqlite.Row
            await db._create_tables()  # type: ignore
            await db.insert_file_record(record)
            assert await db.get_file_record_by_hash("d" * 64) is not None

        # try:
        async with aiosqlite.connect(db_path) as conn:
            db = FileDatabase(db_path)
            db.conn = conn
            db.conn.row_factory = aiosqlite.Row
            with pytest.raises(OverflowError):
                await db.insert_file_record(
                    FileRecord(
                        id=0,
                        file_path=Path("rollback_test") / "test.png",
                        file_hash="e" * 64,
                        file_size=2**63, # fail at n >= 2**63
                        created_at=datetime.now(),
                    ),
                )

        async with aiosqlite.connect(db_path) as conn:
            db = FileDatabase(db_path)
            db.conn = conn
            db.conn.row_factory = aiosqlite.Row
            result = await db.get_file_record_by_hash("e" * 64)
            assert result is None, "Rollback failed to prevent commit"
            existing = await db.get_file_record_by_hash("d" * 64)
            assert existing is not None, "Original record should persist"

    async def test_batch_insert(self, database: FileDatabase, sample_file_record: FileRecord):
        record = sample_file_record
        records: list[FileRecord] = []
        for i, rec in enumerate([record.model_copy() for _ in range(1000)]):
            rec.file_hash = str(i) 
            records.append(rec)
        await database.insert_file_records(records)
        assert await database.get_file_record_by_hash("999") is not None
    
    async def test_count_thousand(self, database: FileDatabase, sample_file_record: FileRecord):
        record = sample_file_record
        records: list[FileRecord] = []
        for i, rec in enumerate([record.model_copy() for _ in range(1000)]):
            rec.file_hash = str(i) 
            records.append(rec)
        await database.insert_file_records(records)
        count = await database.get_count_of_type(record.category)
        assert count == 1000

    async def test_count_zero(self, database: FileDatabase, sample_file_record: FileRecord):
        record = sample_file_record
        count = await database.get_count_of_type(record.category)
        assert count == 0
    
    async def test_unsent_get(self, database: FileDatabase, sample_file_record: FileRecord):
        record = sample_file_record
        record.category = "meme"
        records = await database.get_unsent_files("1", "meme")
        assert len(records) == 0
        await database.insert_file_record(record)
        records = await database.get_unsent_files("1", "meme")
        assert len(records) == 1
        await database.increment_send_count(record.file_hash, "1")
        records = await database.get_unsent_files("1", "meme")
        assert len(records) == 0
        update = await database.increment_send_count(record.file_hash, "1")
        assert update is not None
        assert update.guild_usage["1"].send_count == 2
        records = await database.get_unsent_files("1", "private")
        assert len(records) == 0
        records: list[FileRecord] = []
        for i, rec in enumerate([record.model_copy() for _ in range(1000)]):
            rec.category = "meme"
            rec.file_hash = str(i) 
            records.append(rec)
        await database.insert_file_records(records)
        records = await database.get_unsent_files("1", "meme")
        assert len(records) == 1000
        
    async def test_get_files_larger_than(self, database: FileDatabase, sample_file_record: FileRecord):
        record = sample_file_record
        record.file_size = 1000000
        records = await database.get_files_larger_than(0)
        assert len(records) == 0
        await database.insert_file_record(record)
        records = await database.get_files_larger_than(1000000-1)
        assert len(records) == 1
        records: list[FileRecord] = []
        for i, rec in enumerate([record.model_copy() for _ in range(1000)]):
            rec.file_hash = str(i) 
            records.append(rec)
        await database.insert_file_records(records)
        records = await database.get_files_larger_than(1000000-1)
        assert len(records) == 1001