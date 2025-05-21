"""Utilities for working with files asynchronously."""

import hashlib
import logging
from collections.abc import AsyncGenerator
from pathlib import Path

import aiofiles
import aiofiles.os as aios

HASH_ALGO = hashlib.sha256

logger = logging.getLogger(__name__)


async def hash_file(file_path: Path) -> str:
    """Calculate the hash of the file asynchronously.

    Do not use this function too often, as it can be slow.
    Your disk is your bottleneck.
    The difference in speed of different hashing algorithms is negligible.

    :param file_path: The path to the file to hash.
    :type file_path: Path
    :return: Hexadecimal digest of the file's hash.
    :rtype: str
    """
    async with aiofiles.open(file_path, "rb") as f:
        file_hash = HASH_ALGO()
        while chunk := await f.read():
            file_hash.update(chunk)
    return file_hash.hexdigest()


async def write_to_file(file_path: Path, data: bytes) -> None:
    """Write data to a file asynchronously.

    :param Path file_path: The path to the file to write to.
    :param bytes data: The data to write to the file.
    """
    async with aiofiles.open(file_path, "wb") as f:
        await f.write(data)


async def get_file_size(file_path: Path) -> int:
    """Get the size of the file asynchronously.

    :param file_path: The path to the file whose size is to be determined.
    :type file_path: Path
    :return: The size of the file in bytes.
    :rtype: int
    """
    return (await aios.stat(file_path)).st_size


async def is_file_exists(file_path: Path) -> bool:
    """Check if the file exists asynchronously.

    :param file_path: The path to the file to check.
    :type file_path: Path
    :return: True if the file exists, False otherwise.
    :rtype: bool
    """
    return await aios.path.exists(file_path)


async def remove_file(file_path: Path) -> None:
    """Remove a file asynchronously.

    :param file_path: The path to the file to remove.
    :type file_path: Path
    """
    await aios.remove(file_path)


async def get_files(directory_path: Path) -> AsyncGenerator[Path, None]:
    """Get a list of files in a directory async, including files in subdirectories.

    :param directory_path: The path to the directory whose files are to be listed.
    :type directory_path: Path
    :yield: A generator of file paths in the directory and its subdirectories.
    :rtype: AsyncGenerator[Path, None]
    """
    try:
        if not await aios.path.isdir(directory_path):
            logger.warning(
                "%s is not a directory or does not exist. Skipping.",
                directory_path,
            )
            return
    except OSError:
        logger.warning("Cannot access %s. Skipping.", directory_path)
        return
    for entry in await aios.scandir(directory_path):
        entry_path = Path(entry.path)
        if entry.is_file():
            yield entry_path
        elif entry.is_dir():
            async for sub_entry in get_files(entry_path):
                yield sub_entry


async def get_count_of_files(directory_path: Path) -> int:
    """Get the count of files in a directory async, including files in subdirectories.

    :param directory_path: The path to the directory whose files are to be counted.
    :type directory_path: Path
    :return: The count of files in the directory and its subdirectories.
    :rtype: int
    """
    count = 0
    async for _ in get_files(directory_path):
        count += 1
    return count
