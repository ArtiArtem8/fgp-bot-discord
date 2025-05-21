"""Models for the database and API responses."""

import json
from datetime import datetime
from pathlib import Path

from pydantic import AliasPath, BaseModel, Field, field_serializer, field_validator


class GuildUsage(BaseModel):
    """Guild usage data for a file in the database.

    :param int send_count: Number of times the file was sent in the guild.
    :param datetime | None last_sent: Timestamp of the last time the file was sent.
    """

    send_count: int = 0
    last_sent: datetime | None = None


class FileRecord(BaseModel):
    """Represents a file record in the database.

    :param int | None id: Unique identifier for the file record.
    :param str file_hash: Hash of the file.
    :param Path file_path: Full path to the file, including filename.
    :param int file_size: Size of the file in bytes.
    :param Path | None converted_path: Path to the converted file.
    :param str | None converted_hash: Hash of the converted file.
    :param int | None converted_size: Size of the converted file in bytes.
    :param str category: Category of the file (e.g., 'meme').
    :param dict[str, GuildUsage] guild_usage: Map of guild IDs to their usage data.
    :param datetime created_at: Timestamp when the file record was created.
    """

    id: int | None = None
    file_hash: str
    file_path: Path
    file_size: int
    converted_path: Path | None = None
    converted_hash: str | None = None
    converted_size: int | None = None
    category: str = "meme"
    guild_usage: dict[str, GuildUsage] = Field(default_factory=dict)
    created_at: datetime

    @field_serializer("guild_usage")
    def serialize_guild_usage(self, v: dict[str, GuildUsage]) -> str:
        """Serialize guild_usage as a JSON string.

        :param dict[str, GuildUsage] v: guild_usage dictionary
        :return str: JSON string of serialized guild_usage
        """
        return json.dumps({k: v.model_dump() for k, v in v.items()})

    @field_validator("guild_usage", mode="before")
    @classmethod
    def validate_guild_usage(cls, v: str) -> dict[str, GuildUsage]:
        """Validate guild_usage by parsing JSON string if necessary.

        :param Any v: Input value, either a JSON string or dict.
        :return dict[str, GuildUsage]: Dictionary of GuildUsage objects.
        """
        return {k: GuildUsage(**vv) for k, vv in json.loads(v).items()}


class MediaFile(BaseModel):
    """Represents a media file's technical details from the API response.

    :param int size: File size in bytes
    :param str hash: MD5 hash of the file (aliased from 'md5' in API)
    :param str url: Direct URL to access the file
    :param str extension: File extension (aliased from 'ext' in API)
    """

    size: int = Field(..., description="File size in bytes")
    hash: str = Field(..., validation_alias="md5")
    url: str
    extension: str = Field(..., validation_alias="ext")


class MediaContent(BaseModel):
    """Contains processed content metadata from API response.

    :param int content_id: Unique content identifier (aliased from 'id' in API)
    :param MediaFile file: Detailed file information container
    :param str | None sample_url: URL for sample version if available
    :param str | None preview_url: URL for preview version if available
    :param str rating: Content rating classification
    :param dict[str, list[str]] tags: Categorized taxonomy tags
    """

    content_id: int = Field(..., validation_alias="id")
    file: MediaFile
    sample_url: str | None = Field(None, validation_alias=AliasPath("sample", "url"))
    preview_url: str | None = Field(None, validation_alias=AliasPath("preview", "url"))
    rating: str
    tags: dict[str, list[str]] = Field(default_factory=dict, repr=False)


class ContentResponse(BaseModel):
    """Container for API response containing multiple media content items.

    :param list[MediaContent] posts: List of media content entries
    """

    posts: list[MediaContent]
