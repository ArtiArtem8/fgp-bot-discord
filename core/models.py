"""Models for the database."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, Field


class GuildUsage(BaseModel):
    send_count: int = 0
    last_sent: datetime | None = None


class FileRecord(BaseModel):
    """Represents a file record in the database."""

    id: int | None = None
    file_hash: str
    file_path: Path  # full path with filename
    file_size: int
    converted_path: Path | None = None
    converted_hash: str | None = None
    converted_size: int | None = None
    category: Literal["meme", "private"] | str = "meme"
    guild_usage: dict[str, GuildUsage] = Field(default_factory=dict)
    created_at: datetime

    def model_dump(
        self,
        *,
        mode: Literal["json", "python"] | str = "json",
        **kwargs: Any,
    ) -> dict[str, Any]:
        data = super().model_dump(mode=mode, **kwargs)
        if "guild_usage" in data:
            data["guild_usage"] = json.dumps(data["guild_usage"])
        return data

    @classmethod
    def model_validate(cls, obj: Any, **data: Any) -> Self:
        # Deserialize guild_usage from JSON when creating an instance
        if "guild_usage" in obj:
            guild_usage_text = obj["guild_usage"]
            if isinstance(guild_usage_text, str):
                guild_usage_data = json.loads(guild_usage_text)
                obj["guild_usage"] = {
                    key: GuildUsage(**value) for key, value in guild_usage_data.items()
                }

        return super().model_validate(obj, **data)


# now_utc = datetime.now(UTC)
# record = FileRecord(
#     file_path=Path("/my/data/original.mp4"),
#     file_hash="original_hash_123",
#     file_size=1024000,
#     guild_usage={  # You provide a normal Python dict here
#         "guild_123": GuildUsage(send_count=5, last_sent=now_utc - timedelta(days=1)),
#         "guild_456": GuildUsage(send_count=2, last_sent=now_utc),
#     },
#     created_at=now_utc,
# )

# print(record)
# print(record.model_dump(warnings=True))
# print(record.model_dump_json())
# # build record
# rec = FileRecord.model_validate(record.model_dump(mode="json"))
# print(rec.model_dump(mode="json"))
# print(rec.guild_usage)
# pprint.pprint(rec.model_dump(mode="json"))
# print(FileRecord.__fields__.keys())
# """a = {
#     "id": None,
#     "file_path": WindowsPath("/my/data/original.mp4"),
#     "file_hash": "original_hash_123",
#     "file_size": 1024000,
#     "converted_path": None,
#     "converted_hash": None,
#     "converted_size": None,
#     "guild_usage": {
#         "guild_123": {
#             "send_count": 5,
#             "last_sent": datetime.datetime(
#                 2025,
#                 5,
#                 13,
#                 15,
#                 41,
#                 21,
#                 14325,
#                 tzinfo=datetime.timezone.utc,
#             ),
#         },
#         "guild_456": {
#             "send_count": 2,
#             "last_sent": datetime.datetime(
#                 2025,
#                 5,
#                 14,
#                 15,
#                 41,
#                 21,
#                 14325,
#                 tzinfo=datetime.timezone.utc,
#             ),
#         },
#     },
#     "created_at": datetime.datetime(
#         2025,
#         5,
#         14,
#         15,
#         41,
#         21,
#         14325,
#         tzinfo=datetime.timezone.utc,
#     ),
# }
# b = {
#     "id": null,
#     "file_path": "\\my\\data\\original.mp4",
#     "file_hash": "original_hash_123",
#     "file_size": 1024000,
#     "converted_path": null,
#     "converted_hash": null,
#     "converted_size": null,
#     "guild_usage": {
#         "guild_123": {"send_count": 5, "last_sent": "2025-05-13T15:41:21.014325Z"},
#         "guild_456": {"send_count": 2, "last_sent": "2025-05-14T15:41:21.014325Z"},
#     },
#     "created_at": "2025-05-14T15:41:21.014325Z",
# }
# """
# """{
#     "id": null,
#     "file_path": "\\my\\data\\original.mp4",
#     "file_hash": "original_hash_123",
#     "file_size": 1024000,
#     "converted_path": null,
#     "converted_hash": null,
#     "converted_size": null,
#     "guild_usage": {
#         "guild_123": {"send_count": 5, "last_sent": "2025-05-13T15:44:09.178656Z"},
#         "guild_456": {"send_count": 2, "last_sent": "2025-05-14T15:44:09.178656Z"},
#     },
#     "created_at": "2025-05-14T15:44:09.178656Z",
# }
# {
#     "id": None,
#     "file_path": "\\my\\data\\original.mp4",
#     "file_hash": "original_hash_123",
#     "file_size": 1024000,
#     "converted_path": None,
#     "converted_hash": None,
#     "converted_size": None,
#     "guild_usage": {
#         "guild_123": {"send_count": 5, "last_sent": "2025-05-13T15:44:09.178656Z"},
#         "guild_456": {"send_count": 2, "last_sent": "2025-05-14T15:44:09.178656Z"},
#     },
#     "created_at": "2025-05-14T15:44:09.178656Z",
# }
# """
