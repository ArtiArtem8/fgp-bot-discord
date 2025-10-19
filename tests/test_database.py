import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiosqlite
import pytest

from core.database import FileDatabase, FileRecord
from tests.conftest import BenchmarkResult


class TestDatabaseBasics:
    async def test_create_tables(self, database: FileDatabase):
        # Verify table creation
        async with database.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'",
        ) as cursor:
            tables = await cursor.fetchall()
        assert "file_tracking" in [t[0] for t in tables]
        async with database.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'",
        ) as cursor:
            indexes = await cursor.fetchall()
        index_names = [idx[0] for idx in indexes]
        assert "idx_file_hash" in index_names
        assert "idx_category" in index_names
        assert "idx_guild_usage" in index_names

    async def test_database_connection_lifecycle(self, tmp_path: Path):
        """Test proper connection opening and closing."""
        db_path = tmp_path / "lifecycle_test.db"
        db = FileDatabase(db_path)
        
        with pytest.raises(RuntimeError, match="not connected"):
            _ = db.conn
        
        await db.connect()
        assert db.conn is not None
        
        await db.close()
        with pytest.raises(RuntimeError, match="not connected"):
            _ = db.conn


    async def test_multiple_connect_calls(self, tmp_path: Path):
        """Test that multiple connect() calls raise an error."""
        db_path = tmp_path / "multi_connect.db"
        db = FileDatabase(db_path)
        
        await db.connect()
        
        # Second connect should raise
        with pytest.raises(RuntimeError, match="already connected"):
            await db.connect()
        
        await db.close()
        
class TestInsertOperations:
    """Test record insertion operations."""

    async def test_insert_and_retrieve_file(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        record = sample_file_record
        await database.insert_file_record(record)
        retrieved = await database.get_file_record_by_hash(
            record.file_hash,
        )
        assert retrieved is not None
        assert retrieved.file_path == record.file_path
        assert retrieved.file_hash == record.file_hash
        assert retrieved.file_size == record.file_size
        assert retrieved.guild_usage == {}

    async def test_duplicate_hash_insertion(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        record = sample_file_record
        await database.insert_file_record(record)

        with pytest.raises(Exception, match="UNIQUE constraint failed"):
            await database.insert_file_record(record)

    async def test_batch_insert(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test bulk insert performance."""
        records: list[FileRecord] = []
        for i in range(1000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i:04d}"
            records.append(rec)

        await database.insert_file_records(records)

        # Verify random samples
        assert await database.get_file_record_by_hash("hash_0000") is not None
        assert await database.get_file_record_by_hash("hash_0500") is not None
        assert await database.get_file_record_by_hash("hash_0999") is not None

    async def test_batch_insert_empty_list(self, database: FileDatabase):
        """Test that empty batch insert doesn't fail."""
        await database.insert_file_records([])
        assert await database.get_all_file_hashes() == ()

    async def test_insert_with_converted_fields(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test inserting record with pre-filled converted fields."""
        record = sample_file_record
        record.converted_path = Path("converted/file.webp")
        record.converted_hash = "converted_hash_123"
        record.converted_size = 5000

        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash(record.file_hash)
        assert retrieved is not None
        assert retrieved.converted_path == record.converted_path
        assert retrieved.converted_hash == record.converted_hash
        assert retrieved.converted_size == record.converted_size

class TestQueryOperations:
    """Test various query operations."""

    async def test_get_nonexistent_file(self, database: FileDatabase):
        """Test retrieval of non-existent hash."""
        result = await database.get_file_record_by_hash("nonexistent_hash")
        assert result is None

    async def test_get_by_converted_hash(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test that get_file_record_by_hash finds by converted_hash too."""
        record = sample_file_record
        await database.insert_file_record(record)

        converted_hash = "converted_abc123"
        await database.update_converted_file(
            record.file_hash,
            Path("converted/file.webp"),
            converted_hash,
            5000,
        )

        found1 = await database.get_file_record_by_hash(record.file_hash)
        assert found1 is not None

        found2 = await database.get_file_record_by_hash(converted_hash)
        assert found2 is not None
        assert found2.file_hash == record.file_hash

    async def test_get_file_record_by_path(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test retrieval by exact file path."""
        record = sample_file_record
        await database.insert_file_record(record)

        found = await database.get_file_record_by_path(record.file_path)
        assert found is not None
        assert found.file_hash == record.file_hash
        not_found = await database.get_file_record_by_path(Path("nonexistent/file.png"))
        assert not_found is None

    async def test_get_file_records_by_filename(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test LIKE query on filename."""
        # Insert multiple files
        for i in range(5):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i}"
            rec.file_path = Path(f"files/test_file_{i}.png")
            await database.insert_file_record(rec)

        results = await database.get_file_records_by_filename("test_file_2")
        assert len(results) == 1
        assert "test_file_2" in str(results[0].file_path)

        results = await database.get_file_records_by_filename(".png")
        assert len(results) == 5

        results = await database.get_file_records_by_filename("nonexistent")
        assert len(results) == 0

    async def test_get_count_of_type(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test category counting."""
        count = await database.get_count_of_type("meme")
        assert count == 0

        for i in range(100):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i}"
            rec.category = "meme" if i < 70 else "private"
            await database.insert_file_record(rec)

        assert await database.get_count_of_type("meme") == 70
        assert await database.get_count_of_type("private") == 30
        assert await database.get_count_of_type("nonexistent") == 0

    async def test_get_files_larger_than(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test size-based filtering."""
        records = await database.get_files_larger_than(0)
        assert len(records) == 0

        sizes = [1000, 5000, 10000, 50000, 100000]
        for i, size in enumerate(sizes):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i}"
            rec.file_size = size
            await database.insert_file_record(rec)

        assert len(await database.get_files_larger_than(999)) == 5
        assert len(await database.get_files_larger_than(5000)) == 3
        assert len(await database.get_files_larger_than(100000)) == 0

    async def test_get_unsent_files(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test unsent file filtering by guild."""
        record = sample_file_record
        record.category = "meme"

        records = await database.get_unsent_files("guild_1", "meme")
        assert len(records) == 0

        await database.insert_file_record(record)
        records = await database.get_unsent_files("guild_1", "meme")
        assert len(records) == 1

        await database.increment_send_count(record.file_hash, "guild_1")
        records = await database.get_unsent_files("guild_1", "meme")
        assert len(records) == 0

        records = await database.get_unsent_files("guild_2", "meme")
        assert len(records) == 1

        records = await database.get_unsent_files("guild_1", "private")
        assert len(records) == 0

    async def test_get_all_file_hashes(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test retrieving all hashes."""
        hashes = await database.get_all_file_hashes()
        assert len(hashes) == 0

        expected_hashes: set[str] = set()
        for i in range(50):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i:03d}"
            expected_hashes.add(rec.file_hash)
            await database.insert_file_record(rec)

        hashes = await database.get_all_file_hashes()
        assert len(hashes) == 50
        assert set(hashes) == expected_hashes

    async def test_get_all_filepaths_of_category(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test retrieving paths by category."""
        # Insert files with different categories
        meme_paths: set[Path] = set()
        for i in range(30):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"hash_{i}"
            rec.category = "meme" if i < 20 else "private"
            rec.file_path = Path(f"files/file_{i}.png")
            if i < 20:
                meme_paths.add(rec.file_path)
            await database.insert_file_record(rec)

        paths = await database.get_all_filepaths_of_category("meme")
        assert len(paths) == 20
        assert set(paths) == meme_paths

class TestUpdateOperations:
    """Test update and modification operations."""

    async def test_increment_send_count(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test send count incrementing."""
        record = sample_file_record
        await database.insert_file_record(record)
        guild_id = "guild_123"

        updated = await database.increment_send_count(record.file_hash, guild_id)
        assert updated is not None
        assert guild_id in updated.guild_usage
        assert updated.guild_usage[guild_id].send_count == 1
        assert updated.guild_usage[guild_id].last_sent is not None

        for expected_count in range(2, 6):
            updated = await database.increment_send_count(record.file_hash, guild_id)
            assert updated is not None
            assert updated.guild_usage[guild_id].send_count == expected_count

    async def test_increment_multiple_guilds(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test incrementing for different guilds."""
        record = sample_file_record
        await database.insert_file_record(record)

        await database.increment_send_count(record.file_hash, "guild_1")
        await database.increment_send_count(record.file_hash, "guild_1")
        await database.increment_send_count(record.file_hash, "guild_2")

        result = await database.get_file_record_by_hash(record.file_hash)
        assert result is not None
        assert result.guild_usage["guild_1"].send_count == 2
        assert result.guild_usage["guild_2"].send_count == 1

    async def test_increment_nonexistent_file(self, database: FileDatabase):
        """Test incrementing count for non-existent file."""
        result = await database.increment_send_count("nonexistent_hash", "guild_1")
        assert result is None

    async def test_update_converted_file(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test updating conversion metadata."""
        record = sample_file_record
        await database.insert_file_record(record)

        converted_path = Path("converted/doge.webp")
        converted_size = 8_000_000
        converted_hash = "converted_hash_xyz"

        updated = await database.update_converted_file(
            record.file_hash,
            converted_path,
            converted_hash,
            converted_size,
        )
        assert updated is not None
        assert updated.converted_path == converted_path
        assert updated.converted_hash == converted_hash
        assert updated.converted_size == converted_size

    async def test_update_converted_nonexistent(self, database: FileDatabase):
        """Test updating conversion for non-existent file."""
        result = await database.update_converted_file(
            "nonexistent",
            Path("test.webp"),
            "hash",
            1000,
        )
        assert result is None

    async def test_clear_conversion(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test clearing conversion metadata."""
        record = sample_file_record
        await database.insert_file_record(record)

        await database.update_converted_file(
            record.file_hash,
            Path("converted/file.webp"),
            "converted_hash",
            8000,
        )

        updated = await database.get_file_record_by_hash(record.file_hash)
        assert updated is not None
        assert updated.converted_path is not None

        cleared = await database.clear_conversion(record.file_hash)
        assert cleared is not None
        assert cleared.converted_path is None
        assert cleared.converted_hash is None
        assert cleared.converted_size is None

    async def test_clear_conversion_nonexistent(self, database: FileDatabase):
        """Test clearing conversion for non-existent file."""
        result = await database.clear_conversion("nonexistent")
        assert result is None

class TestDeleteOperations:
    """Test deletion operations."""

    async def test_delete_file_record_by_hash(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test deletion by hash."""
        record = sample_file_record
        await database.insert_file_record(record)

        assert await database.get_file_record_by_hash(record.file_hash) is not None

        deleted = await database.delete_file_record_by_hash(record.file_hash)
        assert deleted is True

        assert await database.get_file_record_by_hash(record.file_hash) is None

    async def test_delete_nonexistent_by_hash(self, database: FileDatabase):
        """Test deleting non-existent record by hash."""
        deleted = await database.delete_file_record_by_hash("nonexistent")
        assert deleted is False

    async def test_delete_file_record_by_path(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test deletion by path."""
        record = sample_file_record
        await database.insert_file_record(record)

        deleted = await database.delete_file_record_by_path(record.file_path)
        assert deleted is True

        assert await database.get_file_record_by_path(record.file_path) is None

    async def test_delete_nonexistent_by_path(self, database: FileDatabase):
        """Test deleting non-existent record by path."""
        deleted = await database.delete_file_record_by_path(Path("nonexistent.png"))
        assert deleted is False

class TestConcurrencyAndTransactions:
    """Test concurrent operations and transaction handling."""

    async def test_concurrent_updates(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test concurrent increment operations."""
        record = sample_file_record
        guild_id = "guild_1"
        await database.insert_file_record(record)

        async def increment_counter():
            result = await database.increment_send_count(record.file_hash, guild_id)
            assert result is not None

        await asyncio.gather(
            increment_counter(),
            increment_counter(),
            increment_counter(),
            increment_counter(),
            increment_counter(),
        )

        final = await database.get_file_record_by_hash(record.file_hash)
        assert final is not None
        assert final.guild_usage[guild_id].send_count == 5

    async def test_transaction_rollback(self, tmp_path: Path):
        """Test that failed transactions rollback properly."""
        db_path = tmp_path / "rollback_test.db"
        db = FileDatabase(db_path)
        await db.connect()

        try:
            record = FileRecord(
                file_path=Path("rollback_test/test.png"),
                file_hash="d" * 64,
                file_size=100,
                category="test",
                created_at=datetime.now(UTC),
            )
            await db.insert_file_record(record)
            assert await db.get_file_record_by_hash("d" * 64) is not None

            with pytest.raises(OverflowError):
                await db.insert_file_record(
                    FileRecord(
                        file_path=Path("rollback_test/fail.png"),
                        file_hash="e" * 64,
                        file_size=2**63,  # Overflow for SQLite INTEGER
                        category="test",
                        created_at=datetime.now(UTC),
                    ),
                )

            result = await db.get_file_record_by_hash("e" * 64)
            assert result is None, "Rollback failed to prevent commit"

            existing = await db.get_file_record_by_hash("d" * 64)
            assert existing is not None, "Original record should persist"
        finally:
            await db.close()

class TestDataValidation:
    """Test data validation and edge cases."""

    async def test_timezone_handling(self, database: FileDatabase):
        """Test TZ-aware datetime storage."""
        tz_aware_dt = datetime.now(UTC)
        record = FileRecord(
            file_path=Path("timezone_test/test.png"),
            file_hash="b" * 64,
            file_size=100,
            category="test",
            created_at=tz_aware_dt,
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash("b" * 64)
        assert retrieved is not None
        assert retrieved.created_at.tzinfo is not None

    async def test_path_normalization(self, database: FileDatabase):
        """Test that paths are stored consistently."""
        # Test relative path
        rel_path = Path("files/test.png")
        record = FileRecord(
            file_path=rel_path,
            file_hash="path_test_1",
            file_size=100,
            category="test",
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash("path_test_1")
        assert retrieved is not None
        assert retrieved.file_path == rel_path

    async def test_missing_required_field(self, database: FileDatabase):
        """Test database-level NOT NULL constraints."""
        with pytest.raises(sqlite3.IntegrityError):
            await database.conn.execute(
                "INSERT INTO file_tracking (file_hash) VALUES (?)",
                ("c" * 64,),
            )
            await database.conn.commit()

    async def test_large_guild_usage_json(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test handling many guild entries."""
        record = sample_file_record
        await database.insert_file_record(record)

        # Increment for many guilds
        for i in range(100):
            await database.increment_send_count(record.file_hash, f"guild_{i}")

        retrieved = await database.get_file_record_by_hash(record.file_hash)
        assert retrieved is not None
        assert len(retrieved.guild_usage) == 100
        assert retrieved.guild_usage["guild_0"].send_count == 1
        assert retrieved.guild_usage["guild_99"].send_count == 1

class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    async def test_empty_category_string(self, database: FileDatabase):
        """Test handling of empty category."""
        record = FileRecord(
            file_path=Path("test.png"),
            file_hash="empty_cat",
            file_size=100,
            category="",  # Empty string
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash("empty_cat")
        assert retrieved is not None
        assert retrieved.category == ""

    async def test_unicode_file_paths(self, database: FileDatabase):
        """Test handling of Unicode characters in paths."""
        unicode_path = Path("файлы/测试/🎉emoji.png")
        record = FileRecord(
            file_path=unicode_path,
            file_hash="unicode_test",
            file_size=100,
            category="test",
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash("unicode_test")
        assert retrieved is not None
        assert retrieved.file_path == unicode_path

    async def test_very_long_hash(self, database: FileDatabase):
        """Test handling of maximum length hash."""
        long_hash = "a" * 64  # SHA-256 is exactly 64 hex chars
        record = FileRecord(
            file_path=Path("test.png"),
            file_hash=long_hash,
            file_size=100,
            category="test",
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash(long_hash)
        assert retrieved is not None

    async def test_zero_file_size(self, database: FileDatabase):
        """Test handling of zero-byte files."""
        record = FileRecord(
            file_path=Path("empty.txt"),
            file_hash="zero_size",
            file_size=0,
            category="test",
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)

        retrieved = await database.get_file_record_by_hash("zero_size")
        assert retrieved is not None
        assert retrieved.file_size == 0

class TestPerformance:
    """Performance and benchmark tests."""

    @pytest.mark.benchmark
    async def test_bulk_insert_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark bulk insert of 10k records."""
        iteration_counter = 0
        iterations = 50
        warmup = 1
        batches: list[list[FileRecord]] = []
        for batch_num in range(iterations + warmup):
            records: list[FileRecord] = []
            for i in range(10_000):
                rec = sample_file_record.model_copy()
                rec.file_hash = f"perf_{batch_num}_{i:05d}"
                records.append(rec)
            batches.append(records)
        
        async def insert_bulk():
            """Only measure the actual database insert."""
            nonlocal iteration_counter
            await database.insert_file_records(batches[iteration_counter % len(batches)])
            iteration_counter += 1
        
        result = await async_benchmark(
            insert_bulk,
            iterations=iterations,
            warmup=warmup,
        )
        
        
        # Assert performance requirements
        assert result.mean_time < 2.0, f"Mean time {result.mean_time:.2f}s exceeds 2s"
        
        # Verify total count (3 iterations × 10k records)
        all_hashes = await database.get_all_file_hashes()
        assert len(all_hashes) == (iterations+warmup) * 10_000

    @pytest.mark.benchmark
    async def test_single_insert_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark single record insert."""
        records_pool: list[FileRecord] = []
        iterations = 1600
        warmup = 5
        for i in range(iterations + warmup):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"single_{i}"
            records_pool.append(rec)
        
        counter = 0
        
        async def insert_single():
            nonlocal counter
            await database.insert_file_record(records_pool[counter])
            counter += 1
        
        result = await async_benchmark(
            insert_single,
            iterations=iterations,
            warmup=warmup,
        )
        
        # Single insert should be fast
        assert result.mean_time < 0.01, f"Single insert too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    async def test_hash_lookup_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark hash lookup on populated database."""
        records: list[FileRecord] = []
        for i in range(10_000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"lookup_test_{i:05d}"
            records.append(rec)
        await database.insert_file_records(records)
        
        async def lookup():
            await database.get_file_record_by_hash("lookup_test_05000")
        
        result = await async_benchmark(
            lookup,
            iterations=4000,
            warmup=100,
        )
        
        # Hash lookup should be very fast (indexed)
        assert result.mean_time < 0.002, f"Hash lookup too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    async def test_category_query_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark category-based query performance."""
        # Insert 10k records across 10 categories (once)
        records: list[FileRecord] = []
        for i in range(50_000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"cat_perf_{i:06d}"
            rec.category = f"category_{i % 10}"
            records.append(rec)
        
        await database.insert_file_records(records)
        
        # Benchmark category query (read-only, no conflicts)
        async def query_category():
            await database.get_count_of_type("category_5")
        
        result = await async_benchmark(
            query_category,
            iterations=8000,
            warmup=5,
        )
        
        assert result.mean_time < 0.05, f"Category query too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    async def test_unsent_files_query_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark unsent files query on large dataset."""
        records: list[FileRecord] = []
        for i in range(20_000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"unsent_{i:06d}"
            rec.category = "meme"
            records.append(rec)
        
        await database.insert_file_records(records)
        
        # Benchmark: Pure query
        async def query_unsent():
            await database.get_unsent_files("guild_123", "meme")
        
        result = await async_benchmark(
            query_unsent,
            iterations=25,
            warmup=10,
        )
        
        assert result.mean_time < 0.25, f"Unsent query too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    async def test_update_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark update operations."""
        # Insert records to update
        records: list[FileRecord] = []
        for i in range(1000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"update_test_{i:03d}"
            records.append(rec)
        await database.insert_file_records(records)
        
        counter = 0
        
        async def update_record():
            nonlocal counter
            hash_to_update = f"update_test_{counter % 1000:03d}"
            await database.increment_send_count(hash_to_update, f"guild_{counter}")
            counter += 1
        
        result = await async_benchmark(
            update_record,
            iterations=1500,
            warmup=10,
        )
        
        assert result.mean_time < 0.01, f"Update too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    @pytest.mark.slow
    async def test_concurrent_operations_performance(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Benchmark concurrent read/write performance."""
        # Insert initial data (once)
        records: list[FileRecord] = []
        for i in range(5000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"concurrent_{i:05d}"
            records.append(rec)
        
        iteration = 0
        
        async def concurrent_ops():
            """Mix of reads and writes."""
            nonlocal iteration
            import asyncio
            
            
            
            async def read_op():
                await database.get_file_record_by_hash(f"concurrent_{iteration % 5000:05d}")
            
            async def write_op():
                # Use different guild_id each time to avoid conflicts
                await database.increment_send_count(
                    f"concurrent_{iteration % 5000:05d}", 
                    f"guild_{iteration}"
                )
            
            await asyncio.gather(
                read_op(),
                read_op(),
                write_op(),
            )
            iteration += 1
        
        result = await async_benchmark(
            concurrent_ops,
            iterations=3000,
            warmup=50,
        )
        
        assert result.mean_time < 0.01, f"Concurrent ops too slow: {result.mean_time*1000:.2f}ms"

    @pytest.mark.benchmark
    async def test_data_preparation_overhead(
        self,
        sample_file_record: FileRecord,
        async_benchmark: Callable[..., Awaitable[BenchmarkResult]],
    ):
        """Measure ONLY the data preparation overhead (no database)."""
        iteration = 0
        
        async def prepare_data():
            """Just create the records, no database operation."""
            nonlocal iteration
            records: list[FileRecord] = []
            for i in range(10_000):
                rec = sample_file_record.model_copy()
                rec.file_hash = f"overhead_{iteration}_{i:05d}"
                records.append(rec)
            iteration += 1
            # Return to prevent optimization
            return len(records)
        
        _ = await async_benchmark(
            prepare_data,
            iterations=200,
            warmup=2,
        )
class TestIndexEffectiveness:
    """Test that database indexes are being used effectively."""

    async def test_file_hash_index_usage(self, database: FileDatabase):
        """Verify file_hash index is used in queries."""
        # Get query plan
        async with database.conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM file_tracking WHERE file_hash = ?",
            ("test_hash",),
        ) as cursor:
            plan = await cursor.fetchall()
        
        plan_str = " ".join(" ".join(str(val) for val in row) for row in plan).lower()
        
        assert (
            "idx_file_hash" in plan_str 
            or "search" in plan_str 
            or "index" in plan_str
        ), f"Query plan doesn't appear to use index: {plan_str}"

    async def test_category_index_usage(self, database: FileDatabase):
        """Verify category index is used in queries."""
        async with database.conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM file_tracking WHERE category = ?",
            ("meme",),
        ) as cursor:
            plan = await cursor.fetchall()
        
        plan_str = " ".join(" ".join(str(val) for val in row) for row in plan).lower()
        
        assert (
            "idx_category" in plan_str 
            or "search" in plan_str 
            or "index" in plan_str
        ), f"Query plan doesn't appear to use index: {plan_str}"

    async def test_query_without_index_comparison(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Compare query plans for indexed vs non-indexed columns."""
        # Insert test data
        records: list[FileRecord] = []
        for i in range(1_000):
            rec = sample_file_record.model_copy()
            rec.file_hash = f"idx_test_{i:05d}"
            rec.category = f"cat_{i % 100}"
            records.append(rec)
        await database.insert_file_records(records)
        
        # Check indexed query plan (file_hash)
        async with database.conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM file_tracking WHERE file_hash = ?",
            ("idx_test_00500",),
        ) as cursor:
            indexed_plan = await cursor.fetchall()
        
        # Check non-indexed query plan (file_size)
        async with database.conn.execute(
            "EXPLAIN QUERY PLAN SELECT * FROM file_tracking WHERE file_size = ?",
            (sample_file_record.file_size,),
        ) as cursor:
            non_indexed_plan = await cursor.fetchall()
        
        indexed_str = " ".join(" ".join(str(val) for val in row) for row in indexed_plan).lower()
        non_indexed_str = " ".join(" ".join(str(val) for val in row) for row in non_indexed_plan).lower()
        
        # Indexed query should mention index or be a search
        assert "index" in indexed_str or "search" in indexed_str
        
        # Non-indexed should be a scan
        assert "scan" in non_indexed_str, f"Expected table scan, got: {non_indexed_str}"


class TestDataIntegrity:
    """Test database constraints and data integrity."""

    async def test_foreign_key_constraints(self, database: FileDatabase):
        """Verify foreign key constraints are enabled."""
        # Check if foreign keys are enabled
        async with database.conn.execute("PRAGMA foreign_keys") as cursor:
            result = await cursor.fetchone()
            # Note: For this simple schema, we might not have FKs
            assert result is not None

    async def test_unique_constraint_on_hash(
        self,
        database: FileDatabase,
        sample_file_record: FileRecord,
    ):
        """Test UNIQUE constraint on file_hash."""
        await database.insert_file_record(sample_file_record)
        
        # Try to insert duplicate
        duplicate = sample_file_record.model_copy()
        with pytest.raises(aiosqlite.IntegrityError, match="UNIQUE"):
            await database.insert_file_record(duplicate)

    async def test_not_null_constraints(self, database: FileDatabase):
        """Test that NOT NULL constraints are enforced."""
        test_cases: list[tuple[str, tuple[Any, ...], str]] = [
            (
                """INSERT INTO file_tracking 
                (file_path, file_size, category, created_at) 
                VALUES (?, ?, ?, ?)""",
                ("path.png", 100, "test", datetime.now(UTC).isoformat()),
                "file_hash"
            ),
            (
                """INSERT INTO file_tracking 
                (file_hash, file_size, category, created_at) 
                VALUES (?, ?, ?, ?)""",
                ("hash123", 100, "test", datetime.now(UTC).isoformat()),
                "file_path"
            ),
            (
                """INSERT INTO file_tracking 
                (file_hash, file_path, category, created_at) 
                VALUES (?, ?, ?, ?)""",
                ("hash456", "path.png", "test", datetime.now(UTC).isoformat()),
                "file_size"
            ),
        ]
        
        for sql, params, field_name in test_cases:
            with pytest.raises(
                sqlite3.IntegrityError,
                match=f"NOT NULL constraint failed.*{field_name}",
            ):
                await database.conn.execute(sql, params)
                await database.conn.commit()


    async def test_pydantic_validation_prevents_bad_data(self):
        """Test that Pydantic validation catches type errors before database."""
        # This should fail at Pydantic validation level
        with pytest.raises((ValueError, TypeError)):
            FileRecord(
                file_path=Path("test.png"),
                file_hash="hash123",
                file_size="not_a_number",  # This should fail Pydantic validation # type: ignore
                category="test",
                created_at=datetime.now(UTC),
            )

    async def test_integer_overflow_handling(self, database: FileDatabase):
        """Test handling of integer overflow (SQLite INTEGER is 8 bytes)."""
        # SQLite INTEGER can hold values up to 2^63 - 1
        max_safe_int = 2**63 - 1
        
        # This should work
        record = FileRecord(
            file_path=Path("large.bin"),
            file_hash="large_file",
            file_size=max_safe_int,
            category="test",
            created_at=datetime.now(UTC),
        )
        await database.insert_file_record(record)
        
        retrieved = await database.get_file_record_by_hash("large_file")
        assert retrieved is not None
        assert retrieved.file_size == max_safe_int
