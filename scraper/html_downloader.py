import aiohttp
import asyncio
import time
import random
from typing import Dict, List, Any, Optional
from logger.logger_interface import LoggerInterface
from logger.console_logger import ConsoleLogger


class AsyncHTMLDownloader:
    """Downloads HTML content from URLs concurrently with retry logic."""

    def __init__(
        self,
        concurrency_limit: int = 10,
        timeout: int = 30,
        retries: int = 3,
        retry_delay: float = 1.0,
        user_agent: str = "COSMOS-Pipeline/1.0",
        logger: Optional[LoggerInterface] = None,
    ):
        """Initialize the HTML downloader.

        Args:
            concurrency_limit: Maximum number of concurrent connections
            timeout: Request timeout in seconds
            retries: Maximum number of retry attempts for failed requests
            retry_delay: Base delay between retries (will be used with exponential backoff)
            user_agent: User-Agent header to use for requests
            logger: External logger implementation (must implement LoggerInterface).
                   If None, a ConsoleLogger will be created automatically.
        """
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.timeout = timeout
        self.retries = retries
        self.retry_delay = retry_delay
        self.user_agent = user_agent
        self.session = None

        # Create default ConsoleLogger if no logger is provided
        self.logger = (
            logger
            if logger is not None
            else ConsoleLogger(service_name="html_downloader")
        )

    async def _ensure_session(self):
        """Ensure that an aiohttp session exists."""
        if self.session is None:
            # Set up connection pooling with limits
            connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
                headers={"User-Agent": self.user_agent},
            )

    async def download_url(self, url_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Download HTML from a single URL with retry logic.

        Args:
            url_metadata: Dictionary containing URL metadata from COSMOS API

        Returns:
            Dictionary with the original metadata plus 'html' and 'download_status' fields
        """
        url = url_metadata.get("url")
        if not url:
            error_msg = f"URL missing in metadata: {url_metadata}"
            await self.logger.error(error_msg, metadata=url_metadata)
            await self.logger.log_failed_download(
                url_metadata=url_metadata, error="Missing URL in metadata"
            )

            return {
                **url_metadata,
                "html": None,
                "download_status": "error",
                "download_error": "Missing URL in metadata",
            }

        # Add a small random delay before each request to avoid overwhelming the server
        # This helps prevent 429 Too Many Requests errors
        await asyncio.sleep(random.uniform(1.0, 3.0))

        # Use semaphore to limit concurrent connections
        async with self.semaphore:
            await self._ensure_session()

            result = {
                **url_metadata,
                "html": None,
                "download_status": "pending",
                "download_error": None,
            }

            for attempt in range(self.retries + 1):
                try:
                    if attempt > 0:
                        # Calculate backoff delay with jitter
                        delay = self.retry_delay * (
                            2 ** (attempt - 1)
                        ) + random.uniform(0, 0.5)
                        await self.logger.info(
                            f"Retry {attempt}/{self.retries} for {url} after {delay:.2f}s delay",
                            url=url,
                            metadata={
                                "attempt": attempt,
                                "max_retries": self.retries,
                                "delay": delay,
                            },
                        )
                        await asyncio.sleep(delay)

                    await self.logger.info(f"Downloading {url}", url=url)
                    start_time = time.time()

                    async with self.session.get(
                        url, allow_redirects=True, ssl=False
                    ) as response:
                        elapsed = time.time() - start_time

                        if response.status == 200:
                            # Success - get the HTML content
                            html = await response.text()
                            success_msg = f"Successfully downloaded {url} ({len(html)} bytes, {elapsed:.2f}s)"
                            await self.logger.info(
                                success_msg,
                                url=url,
                                status=str(response.status),
                                metadata={
                                    "size_bytes": len(html),
                                    "download_time": elapsed,
                                },
                            )

                            result["html"] = html
                            result["download_status"] = "success"
                            result["status_code"] = response.status
                            result["download_time"] = elapsed
                            return result
                        else:
                            error_text = await response.text()
                            error_msg = (
                                f"HTTP {response.status} for {url}: {error_text[:200]}"
                            )
                            await self.logger.warning(
                                error_msg,
                                url=url,
                                status=str(response.status),
                                error=error_text[:200],
                            )

                            # Update result with error details
                            result["download_status"] = "error"
                            result["status_code"] = response.status
                            result["download_error"] = error_msg

                            # Special handling for 429 Too Many Requests
                            if response.status == 429:
                                # Get retry-after header if available, or use a longer backoff
                                retry_after = response.headers.get("Retry-After")
                                if retry_after and retry_after.isdigit():
                                    delay = int(retry_after) + random.uniform(1.0, 3.0)
                                else:
                                    # Use a longer exponential backoff for rate limiting
                                    delay = self.retry_delay * (
                                        2**attempt
                                    ) + random.uniform(5.0, 15.0)

                                await self.logger.warning(
                                    f"Rate limited (429) for {url}. Waiting {delay:.2f}s before retry",
                                    url=url,
                                    status="429",
                                    metadata={"delay": delay},
                                )

                                # Only return if this is the last attempt
                                if attempt >= self.retries:
                                    await self.logger.log_failed_download(
                                        url_metadata=url_metadata,
                                        error=f"Rate limited: {error_msg}",
                                        status_code=response.status,
                                    )
                                    return result

                                # Otherwise wait the calculated delay
                                await asyncio.sleep(delay)
                                continue  # Skip to next retry attempt

                            # Don't retry for certain status codes
                            if response.status in (404, 403, 401):
                                await self.logger.warning(
                                    f"Not retrying {url} due to status code {response.status}",
                                    url=url,
                                    status=str(response.status),
                                )

                                await self.logger.log_failed_download(
                                    url_metadata=url_metadata,
                                    error=error_msg,
                                    status_code=response.status,
                                )

                                return result

                            # For other error codes, continue to retry

                except asyncio.TimeoutError:
                    error_msg = f"Timeout downloading {url}"
                    await self.logger.warning(
                        error_msg, url=url, error=f"Timeout after {self.timeout}s"
                    )
                    result["download_status"] = "error"
                    result["download_error"] = f"Timeout after {self.timeout}s"

                except aiohttp.ClientError as e:
                    error_msg = f"Error downloading {url}: {str(e)}"
                    await self.logger.warning(error_msg, url=url, error=str(e))
                    result["download_status"] = "error"
                    result["download_error"] = f"Client error: {str(e)}"

                except Exception as e:
                    error_msg = f"Unexpected error downloading {url}: {str(e)}"
                    await self.logger.error(error_msg, url=url, error=str(e))
                    result["download_status"] = "error"
                    result["download_error"] = f"Unexpected error: {str(e)}"

            # If we get here, all retries failed
            all_failed_msg = f"All {self.retries + 1} attempts failed for {url}"
            await self.logger.error(all_failed_msg, url=url)

            await self.logger.log_failed_download(
                url_metadata=url_metadata,
                error=result.get("download_error", "All retry attempts failed"),
                status_code=result.get("status_code"),
            )

            return result

    async def download_batch(
        self, url_metadata_list: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Download HTML from multiple URLs concurrently.

        Args:
            url_metadata_list: List of URL metadata dictionaries from COSMOS API

        Returns:
            List of dictionaries with original metadata plus HTML content and status
        """
        await self._ensure_session()

        if not url_metadata_list:
            await self.logger.warning("Empty URL batch provided")
            return []

        await self.logger.info(
            f"Downloading batch of {len(url_metadata_list)} URLs",
            metadata={"batch_size": len(url_metadata_list)},
        )
        start_time = time.time()

        # Create download tasks for all URLs
        tasks = [self.download_url(url_metadata) for url_metadata in url_metadata_list]

        # Process all download tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any unexpected exceptions
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                error_msg = f"Unhandled exception downloading URL: {str(result)}"
                await self.logger.error(
                    error_msg, url=url_metadata_list[i].get("url"), error=str(result)
                )

                # Create an error result using the original metadata
                error_result = {
                    **url_metadata_list[i],
                    "html": None,
                    "download_status": "error",
                    "download_error": f"Unhandled exception: {str(result)}",
                }

                await self.logger.log_failed_download(
                    url_metadata=url_metadata_list[i],
                    error=f"Unhandled exception: {str(result)}",
                )

                processed_results.append(error_result)
            else:
                processed_results.append(result)

        elapsed = time.time() - start_time
        success_count = sum(
            1 for r in processed_results if r.get("download_status") == "success"
        )
        failed_count = len(processed_results) - success_count

        await self.logger.info(
            f"Completed batch download: {success_count}/{len(url_metadata_list)} successful in {elapsed:.2f}s",
            metadata={
                "success_count": success_count,
                "failed_count": failed_count,
                "total_count": len(url_metadata_list),
                "elapsed_time": elapsed,
            },
        )

        return processed_results

    async def close(self):
        """Close the aiohttp session and logger."""
        if self.session is not None:
            await self.session.close()
            self.session = None

        # Close the logger
        await self.logger.close()


# Example usage
async def main():
    import asyncio

    # Create sample URL metadata (normally this would come from CosmosAPIClient)
    sample_urls = [
        {
            "url": "https://data.minorplanetcenter.net/",
            "title": "The International Astronomical Union Minor Planet Center",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        },
        {
            "url": "https://data.minorplanetcenter.net/comparison/index.html",
            "title": "Orbit Comparison Tool",
            "document_type": "Software and Tools",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        },
        {
            "url": "https://data.minorplanetcenter.net/db_search",
            "title": "MPC Database Search",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        },
        {
            "url": "https://data.minorplanetcenter.net/explorer/",
            "title": "The International Astronomical Union Minor Planet Center",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        },
        {
            "url": "https://data.minorplanetcenter.net/iau/AboutThisService.html",
            "title": "IAU Minor Planet Center - About This Service",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        },
    ]

    # Initialize downloader (will use ConsoleLogger by default)
    downloader = AsyncHTMLDownloader(concurrency_limit=5, timeout=10, retries=2)

    try:
        # Download HTML for the sample URLs
        results = await downloader.download_batch(sample_urls)

        # Print results
        for result in results:
            if result["download_status"] == "success":
                html_preview = result["html"][:100] + "..." if result["html"] else None
                print(f"\nSuccessfully downloaded: {result['url']}")
                print(f"HTML preview: {html_preview}")
                print(f"Download time: {result.get('download_time', 'N/A')}s")
            else:
                print(f"\nFailed to download: {result['url']}")
                print(f"Error: {result.get('download_error', 'Unknown error')}")
                print(f"Status code: {result.get('status_code', 'N/A')}")

    finally:
        await downloader.close()


if __name__ == "__main__":
    asyncio.run(main())
