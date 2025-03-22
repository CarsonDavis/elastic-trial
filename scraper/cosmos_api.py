import aiohttp
import asyncio
import logging
from typing import Dict, List, Any, AsyncGenerator, Optional
from logger.logger_interface import LoggerInterface
from logger.elasticsearch_logger import ElasticsearchLogger
from logger.console_logger import ConsoleLogger


class CosmosAPIClient:
    """Handles interaction with the COSMOS API to fetch URL metadata."""

    def __init__(
        self,
        base_url: str = "https://sde-indexing-helper.nasa-impact.net/candidate-urls-api",
        logger: Optional[LoggerInterface] = None,
    ):
        """Initialize the COSMOS API client.

        Args:
            base_url: Base URL for the COSMOS API
            logger: External logger implementation (must implement LoggerInterface)
        """
        self.base_url = base_url.rstrip("/")
        self.session = None
        self.logger = logger

        # Set up standard Python logger as fallback
        self.std_logger = logging.getLogger(__name__)
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.std_logger.addHandler(handler)
        self.std_logger.setLevel(logging.INFO)

    async def _ensure_session(self):
        """Ensure that an aiohttp session exists."""
        if self.session is None:
            self.session = aiohttp.ClientSession()

    async def _log(self, level: str, message: str, **kwargs):
        """Log using the provided logger or fallback to standard logging."""
        # Standard logging as fallback
        log_method = getattr(self.std_logger, level.lower(), self.std_logger.info)
        log_method(message)

        # Use external logger if provided
        if self.logger:
            logger_method = getattr(self.logger, level.lower(), self.logger.info)
            await logger_method(message, **kwargs)

    async def stream_collection_urls(
        self, collection_name: str, batch_size: int = 100
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Stream URLs for a specific collection with pagination.

        Instead of returning all URLs at once, this yields batches of URLs
        to prevent memory issues with large collections.

        Args:
            collection_name: The exact endpoint name in the COSMOS API
            batch_size: Number of URLs to fetch per request

        Yields:
            Batches of URL metadata dictionaries
        """
        await self._ensure_session()

        endpoint = f"{self.base_url}/{collection_name}/"
        await self._log("info", f"Streaming URLs from endpoint: {endpoint}")

        next_page_url = f"{endpoint}?page_size={batch_size}"
        page_num = 1
        total_yielded = 0

        while next_page_url:
            try:
                await self._log("info", f"Fetching page {page_num}: {next_page_url}")

                async with self.session.get(next_page_url) as response:
                    if response.status != 200:
                        text = await response.text()
                        error_msg = f"Error response ({response.status}): {text[:500]}"
                        await self._log(
                            "error",
                            error_msg,
                            status=str(response.status),
                            url=next_page_url,
                        )
                        response.raise_for_status()

                    data = await response.json()

                    # Extract results and yield this batch
                    if "results" in data and data["results"]:
                        results = data["results"]
                        total_yielded += len(results)
                        await self._log(
                            "info",
                            f"Yielding batch of {len(results)} URLs, total so far: {total_yielded}",
                            metadata={
                                "batch_size": len(results),
                                "total_urls": total_yielded,
                            },
                        )
                        yield results
                    else:
                        await self._log(
                            "warning", "No results in current page", url=next_page_url
                        )
                        break

                    # Get next page URL if it exists
                    next_page_url = data.get("next")
                    page_num += 1

            except aiohttp.ClientError as e:
                error_msg = f"Error fetching URLs: {str(e)}"
                await self._log("error", error_msg, url=next_page_url, error=str(e))
                raise

        await self._log(
            "info",
            f"Completed streaming a total of {total_yielded} URLs",
            metadata={"total_urls": total_yielded},
        )

    async def close(self):
        """Close the aiohttp session."""
        if self.session is not None:
            await self.session.close()
            self.session = None

        # No need to close the logger here, as it's managed externally


async def example_usage():
    import asyncio
    import os
    from logger.elasticsearch_logger import ElasticsearchLogger
    from logger.console_logger import ConsoleLogger

    # Get configuration from environment variables
    es_host = os.getenv(
        "ES_HOST",
        "https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
    )
    es_api_key = os.getenv("ES_API_KEY")
    cosmos_api_url = os.getenv(
        "COSMOS_API_URL",
        "https://sde-indexing-helper.nasa-impact.net/candidate-urls-api",
    )

    # Choose which logger to use based on environment variable
    logger_type = os.getenv("LOGGER_TYPE", "console").lower()

    if logger_type == "elasticsearch" and es_host and es_api_key:
        print("Using Elasticsearch logger")
        # Create Elasticsearch logger
        logger = ElasticsearchLogger(
            host=es_host,
            index_prefix="sde_logs",
            service_name="cosmos_api",
            api_key=es_api_key,
        )
    else:
        print("Using Console logger")
        # Create console logger
        logger = ConsoleLogger(service_name="cosmos_api", log_level="INFO")

    # Create the COSMOS API client with the chosen logger
    # No need to import CosmosAPIClient as we're already in that file
    cosmos_client = CosmosAPIClient(base_url=cosmos_api_url, logger=logger)

    try:
        # Use the client to fetch data from a collection
        collection_name = "iau_minor_planet_system"
        batch_size = 20
        batch_count = 0

        print(f"Fetching data from collection: {collection_name}")

        # Process a few batches as an example
        async for batch in cosmos_client.stream_collection_urls(
            collection_name, batch_size
        ):
            batch_count += 1
            print(f"Received batch {batch_count} with {len(batch)} URLs")

            # Just process the first 3 batches as an example
            if batch_count >= 3:
                break

        print(f"Processed {batch_count} batches")

    finally:
        # Close connections
        await cosmos_client.close()
        await logger.close()


if __name__ == "__main__":
    import asyncio

    asyncio.run(example_usage())
