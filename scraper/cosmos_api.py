import aiohttp
import asyncio
import logging
from typing import Dict, List, Any, AsyncGenerator


class CosmosAPIClient:
    """Handles interaction with the COSMOS API to fetch URL metadata."""

    def __init__(
        self,
        base_url: str = "https://sde-indexing-helper.nasa-impact.net/candidate-urls-api",
    ):
        """Initialize the COSMOS API client."""
        self.base_url = base_url.rstrip("/")
        self.session = None
        self.logger = logging.getLogger(__name__)

        # Configure logging
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    async def _ensure_session(self):
        """Ensure that an aiohttp session exists."""
        if self.session is None:
            self.session = aiohttp.ClientSession()

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
        self.logger.info(f"Streaming URLs from endpoint: {endpoint}")

        next_page_url = f"{endpoint}?page_size={batch_size}"
        page_num = 1
        total_yielded = 0

        while next_page_url:
            try:
                self.logger.info(f"Fetching page {page_num}: {next_page_url}")

                async with self.session.get(next_page_url) as response:
                    if response.status != 200:
                        text = await response.text()
                        self.logger.error(
                            f"Error response ({response.status}): {text[:500]}"
                        )
                        response.raise_for_status()

                    data = await response.json()

                    # Extract results and yield this batch
                    if "results" in data and data["results"]:
                        results = data["results"]
                        total_yielded += len(results)
                        self.logger.info(
                            f"Yielding batch of {len(results)} URLs, total so far: {total_yielded}"
                        )
                        yield results
                    else:
                        self.logger.warning("No results in current page")
                        break

                    # Get next page URL if it exists
                    next_page_url = data.get("next")
                    page_num += 1

            except aiohttp.ClientError as e:
                self.logger.error(f"Error fetching URLs: {str(e)}")
                raise

        self.logger.info(f"Completed streaming a total of {total_yielded} URLs")

    async def close(self):
        """Close the aiohttp session."""
        if self.session is not None:
            await self.session.close()
            self.session = None


# Example usage demonstrating streaming approach
async def main():
    # Create and configure the client
    client = CosmosAPIClient()

    try:
        # Stream URLs for the IAU Minor Planet System collection
        collection_endpoint = "iau_minor_planet_system"

        # Counter for demonstration
        total_processed = 0
        first_url_shown = False

        # Process URLs in batches
        async for url_batch in client.stream_collection_urls(collection_endpoint):
            # Here you would pass each batch to the next stage of the pipeline
            # instead of accumulating all URLs in memory

            batch_size = len(url_batch)
            total_processed += batch_size

            # Just for demo, show the first URL object
            if not first_url_shown and batch_size > 0:
                print("\nFirst URL object in first batch:")
                print(f"URL: {url_batch[0]['url']}")
                print(f"Title: {url_batch[0]['title']}")
                print(f"Document Type: {url_batch[0]['document_type']}")
                print(f"File Extension: {url_batch[0]['file_extension']}")
                print(f"Tree Root: {url_batch[0]['tree_root']}")
                print(f"TDAMM Tags: {url_batch[0]['tdamm_tag']}")
                first_url_shown = True

            print(
                f"Processed batch of {batch_size} URLs, running total: {total_processed}"
            )

            # Here you would process each batch
            # For example: await process_url_batch(url_batch)

        print(f"\nTotal URLs processed: {total_processed}")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
