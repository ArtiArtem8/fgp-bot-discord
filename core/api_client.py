"""Module for interacting with the API asynchronously.

This module provides the MediaAPIClient class, which allows to get media and tags
from the API while respecting rate limits and handling authentication.
"""

import asyncio
import base64
import logging
import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import aiohttp
from aiolimiter import AsyncLimiter

from core.enums import Category, DateRange, FileType, Rating, SortOrder
from core.models import ContentResponse, TagResponse

logger = logging.getLogger(__name__)

ResponseData = dict[str, Any] | bytes | list[dict[str, Any]]
ResponseResult = tuple[int, ResponseData]


@dataclass(frozen=True)
class ContentParams:
    """Class to hold the parameters for fetching content from the API.

    :param list[str] | None tags: List of tags to filter posts (default: None)
    :param Rating | None rating: Content rating filter (default: None)
    :param FileType | None file_type: File type filter (default: None)
    :param SortOrder | None sort_order: Sort order for posts (default: None)
    """

    tags: list[str] | None = None
    rating: Rating | None = None
    file_type: FileType | None = None
    sort_order: SortOrder | None = None
    date_range: DateRange | None = None

    def build_tags(self) -> str:
        """Build the combined tag string for API requests.

        :return str: formatted tags string
        """
        tags = self.tags.copy() if self.tags else []

        # Add new filters
        if self.rating:
            tags.append(f"rating:{self.rating.lower()}")
        if self.file_type:
            tags.append(f"type:{self.file_type.lower()}")
        if self.sort_order:
            tags.append(f"order:{self.sort_order.lower()}")
        if self.date_range:
            tags.append(f"date:{self.date_range.lower()}")

        return " ".join(tags).strip()


class APIError(Exception):
    """Exception raised for errors in API."""


class APIConfig:
    """Configuration class for APIClient."""

    def __init__(self) -> None:
        """Initialize the configuration with environment variables or defaults."""
        self.username: str | None = os.getenv("MEDIA_USERNAME")
        self.api_key: str | None = os.getenv("MEDIA_API_KEY")
        if (user_agent := os.getenv("MEDIA_USER_AGENT")) is None:
            msg = "User-Agent must be provided"
            raise ValueError(msg)
        self.user_agent: str = user_agent
        self.base_url: str | None = os.getenv("MEDIA_BASE_URL")
        self.max_requests: int = int(os.getenv("MEDIA_MAX_REQUESTS", "2"))
        self.max_workers: int = int(os.getenv("MEDIA_MAX_WORKERS", "2"))
        self.request_timeout: int = int(os.getenv("MEDIA_REQUEST_TIMEOUT", "10"))


class MediaAPIClient:
    """Asynchronous client for interacting with API.

    ## Example Usage
    .. code:: python

        async def example_usage():
            config = APIConfig()
            client = MediaAPIClient(config)

            results = await client.get_content(
                tags=["animal", "nature"], rating="s", limit=5
            )

            for item in results:
                print(f"Found media ID: {item.content_id}")
    """

    def __init__(self, config: APIConfig) -> None:
        """Initialize the MediaAPIClient with the given configuration.

        :param APIConfig config: Configuration object with API credentials and settings
        """
        self.config = config
        self.queue: asyncio.Queue[
            tuple[str, dict[str, Any], asyncio.Future[ResponseResult]]
        ] = asyncio.Queue()
        self.limiter = AsyncLimiter(1, 3)
        self.semaphore = asyncio.Semaphore(self.config.max_workers)
        self.session = aiohttp.ClientSession(
            headers=self._create_headers(),
            timeout=aiohttp.ClientTimeout(self.config.request_timeout),
        )
        self.workers: list[asyncio.Task[None]] = [
            asyncio.create_task(self._worker()) for _ in range(self.config.max_workers)
        ]
        logger.debug("API client initialized")

    def _create_headers(self) -> dict[str, str]:
        """Create headers for API requests, including authentication.

        :return Dict[str, str]: Headers with User-Agent and Authorization
        :raises ValueError: If username or API key is missing
        """
        if not self.config.username or not self.config.api_key:
            msg = "Username and API key must be provided"
            raise ValueError(msg)
        auth: str = base64.b64encode(
            f"{self.config.username}:{self.config.api_key}".encode(),
        ).decode()
        return {
            "User-Agent": self.config.user_agent,
            "Authorization": f"Basic {auth}",
        }

    async def _worker(self) -> None:
        """Process requests from the queue indefinitely.

        This method runs a worker loop that handles queued requests while respecting
        rate limits and concurrency constraints.
        """
        while True:
            future = None
            url = None
            try:
                async with self.semaphore:
                    url, params, future = await self.queue.get()
                    try:
                        async with self.limiter:
                            await self._process_request(url, params, future)
                    except Exception as e:
                        logger.exception("Worker error on %s.", url)
                        if future and not future.done():
                            future.set_exception(e)
                    else:
                        self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("Worker error on %s.", url)
                if future and not future.done():
                    future.set_exception(e)

    async def _process_request(
        self,
        url: str,
        params: dict[str, Any],
        future: asyncio.Future[ResponseResult],
    ) -> None:
        """Process a single API request and set the result on the future.

        :param str url: The API endpoint URL
        :param dict[str, Any] params: Query parameters for the request
        :param asyncio.Future future: Future to set the result or exception
        """
        logger.debug("Processing request: %s", url)
        async with self.session.get(url=url, params=params) as response:
            if response.status in (
                HTTPStatus.TOO_MANY_REQUESTS,
                HTTPStatus.SERVICE_UNAVAILABLE,
            ):
                future.set_exception(Exception("API rate limit exceeded"))
                return
            response.raise_for_status()

            content_type: str = response.headers.get("Content-Type", "")
            data: ResponseData = (
                await response.json()
                if "application/json" in content_type
                else await response.read()
            )
            future.set_result((response.status, data))

    async def get_content(
        self,
        limit: int = 10,
        content_params: ContentParams | None = None,
        page: str | None = None,
    ) -> ContentResponse:
        """Fetch content from the API with the specified parameters.

        :param int limit: Maximum number of posts to retrieve (default: 10, max: 320)
        :param ContentParams | None content_params: Content parameters (default: None)
        :param str | None page: Page number or cursor for pagination (default: None)
        :return ContentResponse: Model containing the API response
        :raises Exception: If the API request fails or returns an error
        :raises APIError: If the API response is not a dictionary
        """
        if content_params is None:
            content_params = ContentParams()  # defaults are none
        params: dict[str, int | str] = {
            "limit": min(limit, 320),
            "tags": content_params.build_tags(),
        }
        if page is not None:
            params["page"] = page
        logger.debug("Fetching posts with params: %s", params)
        data = await self._enqueue_request("posts.json", params)
        if not isinstance(data, dict):
            msg = "API response is not a dictionary"
            raise APIError(msg)
        logger.debug("Fetched posts: %s", data)
        return ContentResponse.model_validate(data)

    async def get_tags(
        self,
        search: str | None = None,
        category: Category | None = None,
        order: str = "count",
        limit: int = 75,
    ) -> TagResponse:
        """Fetch tags from the API with the specified parameters.

        :param str | None search: Search term for tag names (default: None)
        :param Category | None category: Category filter for tags (default: None)
        :param str order: Order of the tags (default: "count")
        :param int limit: Maximum number of tags to retrieve (default: 75, max: 320)
        :return TagResponse: TagResponse containing the API response
        :raises Exception: If the API request fails or returns an error
        :raises APIError: If the API response is not a dictionary
        """
        params: dict[str, bool | int | str] = {
            "search[order]": order,
            "search[hide_empty]": "true",
            # "search[status]": "active",
            "limit": min(limit, 320),
        }
        if search is not None:
            params["search[name_matches]"] = search
        if category is not None:
            params["search[category]"] = category
        data = await self._enqueue_request("tags.json", params)
        if not isinstance(data, (dict, list)):
            msg = "API response is not a dictionary"
            raise APIError(msg)
        logger.debug("Fetched tags: %s", data)
        return TagResponse.model_validate(data)

    async def download_file(self, url: str) -> bytes:
        """Download a file from the API and return the binary data.

        :param str url: URL of the file to download
        :return bytes: Binary data of the downloaded file
        :raises Exception: If the API request fails or returns an error
        :raises APIError: If the response is not bytes
        """
        response = await self._enqueue_url_request(url)
        if not isinstance(response, bytes):
            msg = "Invalid response"
            raise APIError(msg)
        return response

    async def _enqueue_request(
        self,
        endpoint: str | None,
        params: dict[str, Any],
    ) -> ResponseData:
        """Enqueue a request and await the response.

        :param str endpoint: API endpoint (e.g., "posts.json")
        :param dict[str, Any] params: Query parameters for the request
        :return ResponseData: Processed API response
        :raises Exception: If the connection fails
        :raises APIError: If the API status code indicates an error
        """
        future: asyncio.Future[ResponseResult] = asyncio.Future()
        url = f"{self.config.base_url}/{endpoint}"
        await self.queue.put(
            (url, {k: v for k, v in params.items() if v is not None}, future),
        )
        logger.debug("Enqueued request: %s", url)
        status, data = await future
        return self._process_response(data, status)

    async def _enqueue_url_request(self, url: str) -> ResponseData:
        future: asyncio.Future[ResponseResult] = asyncio.Future()
        await self.queue.put((url, {}, future))
        status, data = await future
        return self._process_response(data, status)

    def _process_response(self, data: ResponseData, status: int) -> ResponseData:
        """Process the API response and handle errors.

        :param Any data: Response data from the API
        :param int status: HTTP status code
        :return ResponseData: Parsed response data
        :raises APIError: If the status code indicates an error
        """
        if HTTPStatus(status).is_success:
            return data
        reason = (
            data.get("reason", "Unknown error")
            if isinstance(data, dict)
            else "Unknown error"
        )
        msg = f"API Error {HTTPStatus(status)}: {reason}"
        raise APIError(msg)

    async def close(self) -> None:
        """Close the client, canceling workers and closing the session."""
        await self.queue.join()
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        await self.session.close()
