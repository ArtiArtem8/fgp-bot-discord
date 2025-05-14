from datetime import UTC, datetime
import hashlib
from pathlib import Path

import pytest_asyncio

from core.database import FileDatabase
from core.models import FileRecord


@pytest_asyncio.fixture(autouse=True, scope="function")
async def test_db_path(tmp_path: Path) -> Path:
    return tmp_path / "test.db"


@pytest_asyncio.fixture(autouse=True, scope="function")
async def database(test_db_path: Path) -> FileDatabase:  # type: ignore  # noqa: PGH003
    db = FileDatabase(test_db_path)
    await db.connect()
    yield db # type: ignore
    await db.conn.close()


@pytest_asyncio.fixture(autouse=True, scope="function")
async def sample_file_record() -> FileRecord:
    return FileRecord(
        file_path=Path("original/doge.png"),
        file_hash=hashlib.sha256(b"original/doge.png").hexdigest(),
        file_size=12345,
        created_at=datetime.now(UTC),
    )
