"""Various enums used in the application."""

from enum import IntEnum, StrEnum


class Category(IntEnum):
    """Represents content category classifications.

    :param GENERAL: General content (0)
    :param ARTIST: Artist content (1)
    :param COPYRIGHT: Copyright content (3)
    :param CHARACTER: Character content (4)
    :param SPECIES: Species content (5)
    :param INVALID: Invalid content (6)
    :param META: Meta content (7)
    :param LORE: Lore content (8)
    """

    GENERAL = 0
    ARTIST = 1
    COPYRIGHT = 3
    CHARACTER = 4
    SPECIES = 5
    INVALID = 6
    META = 7
    LORE = 8


class Rating(StrEnum):
    """Represents content rating classifications.

    :param SAFE: Safe for work content ('s')
    :param QUESTIONABLE: Questionable content ('q')
    :param EXPLICIT: Explicit content ('e')
    """

    SAFE = "s"
    QUESTIONABLE = "q"
    EXPLICIT = "e"


class FileType(StrEnum):
    """Represents file types for posts.

    :param JPG: Posts that are JPG, a type of image
    :param PNG: Posts that are PNG, a type of image
    :param GIF: Posts that are GIF, a type of image (may be animated)
    :param SWF: Posts that are Flash, a format used for animation
    :param WEBM: Posts that are WebM, a type of video
    """

    JPG = "jpg"
    PNG = "png"
    GIF = "gif"
    WEBM = "webm"


class SortOrder(StrEnum):
    """Represents sorting criteria and order for posts.

    :param SCORE: Highest score first
    :param FAVCOUNT: Most favorites first
    :param TAGCOUNT: Most tags first
    :param COMMENT_COUNT: Most comments first
    :param COMMENT_BUMPED: Posts with the newest comments
    :param LANDSCAPE: Wide and short to tall and thin
    :param PORTRAIT: Tall and thin to wide and short
    :param DURATION: Video duration longest to shortest
    :param ID: Oldest to newest
    :param FILESIZE: Largest file size first
    :param MPIXELS: Largest resolution first
    :param RANDOM: Orders posts randomly
    :param SCORE_ASC: Lowest score first
    :param FAVCOUNT_ASC: Least favorites first
    :param TAGCOUNT_ASC: Least tags first
    :param COMMENT_COUNT_ASC: Least comments first
    :param COMMENT_BUMPED_ASC: Posts that have not been commented for the longest time
    :param MPIXELS_ASC: Smallest resolution first
    :param FILESIZE_ASC: Smallest file size first
    :param DURATION_ASC: Video duration shortest to longest
    """

    ID = "id"
    RANDOM = "random"
    SCORE = "score"
    SCORE_ASC = "score_asc"
    FAVCOUNT = "favcount"
    FAVCOUNT_ASC = "favcount_asc"
    TAGCOUNT = "tagcount"
    TAGCOUNT_ASC = "tagcount_asc"
    COMMENT_COUNT = "comment_count"
    COMMENT_COUNT_ASC = "comment_count_asc"
    COMMENT_BUMPED = "comment_bumped"
    COMMENT_BUMPED_ASC = "comment_bumped_asc"
    MPIXELS = "mpixels"
    MPIXELS_ASC = "mpixels_asc"
    FILESIZE = "filesize"
    FILESIZE_ASC = "filesize_asc"
    LANDSCAPE = "landscape"
    PORTRAIT = "portrait"
    DURATION = "duration"
    DURATION_ASC = "duration_asc"


class DateRange(StrEnum):
    """Represents date ranges for filtering posts.

    Simple time period

    :param DAY: Posts from the last day
    :param WEEK: Posts from the last 7 days
    :param MONTH: Posts from the last 30 days
    :param YEAR: Posts from the last 365 days
    :param DECADE: Posts from the last decade
    """

    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    DECADE = "decade"
