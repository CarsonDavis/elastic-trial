#!/usr/bin/env python3
"""
Cosmos to Elasticsearch Pipeline

This script orchestrates the complete pipeline for fetching URLs from the COSMOS API,
downloading their HTML content, and indexing them in Elasticsearch.
"""

import asyncio
import argparse
import os
import time
from typing import Dict, List, Any, Optional

# Import components
from cosmos_api import CosmosAPIClient
from html_downloader import AsyncHTMLDownloader
from html_processor import HTMLProcessor
from elastic_upload import ElasticsearchClient
from logger.elasticsearch_logger import ElasticsearchLogger
from logger.console_logger import ConsoleLogger
from logger.logger_interface import LoggerInterface


class Pipeline:
    """Orchestrates the entire COSMOS to Elasticsearch pipeline."""

    def __init__(
        self,
        cosmos_api_url: str,
        elasticsearch_host: str,
        elasticsearch_index: str,
        elasticsearch_api_key: Optional[str] = None,
        concurrency: int = 10,
        batch_size: int = 20,
        logger: Optional[LoggerInterface] = None,
    ):
        """Initialize the pipeline.

        Args:
            cosmos_api_url: Base URL for the COSMOS API
            elasticsearch_host: Elasticsearch host URL
            elasticsearch_index: Name of the index to use
            elasticsearch_api_key: API key for Elasticsearch authentication
            concurrency: Maximum number of concurrent downloads
            batch_size: Number of URLs to process in each batch
            logger: Logger implementation to use for the pipeline itself
        """
        # Set up the pipeline's own logger
        self.logger = (
            logger
            if logger is not None
            else ConsoleLogger(service_name="pipeline_orchestrator")
        )

        # Create component-specific loggers
        if isinstance(logger, ElasticsearchLogger):
            # If ES logger was provided, create component-specific ones with the same config
            es_host = logger.host
            es_index_prefix = logger.index_prefix
            es_api_key = logger.api_key

            self.cosmos_logger = ElasticsearchLogger(
                host=es_host,
                index_prefix=es_index_prefix,
                service_name="cosmos_api",
                api_key=es_api_key,
            )

            self.downloader_logger = ElasticsearchLogger(
                host=es_host,
                index_prefix=es_index_prefix,
                service_name="html_downloader",
                api_key=es_api_key,
            )

            self.processor_logger = ElasticsearchLogger(
                host=es_host,
                index_prefix=es_index_prefix,
                service_name="html_processor",
                api_key=es_api_key,
            )

            self.es_client_logger = ElasticsearchLogger(
                host=es_host,
                index_prefix=es_index_prefix,
                service_name="elasticsearch_client",
                api_key=es_api_key,
            )
        else:
            # For console logger or other types, create new ones with appropriate service names
            log_level = "INFO"
            if isinstance(logger, ConsoleLogger) and hasattr(logger, "logger"):
                # Get log level from the provided logger if possible
                log_level = logger.logger.level

            self.cosmos_logger = ConsoleLogger(
                service_name="cosmos_api", log_level=log_level
            )
            self.downloader_logger = ConsoleLogger(
                service_name="html_downloader", log_level=log_level
            )
            self.processor_logger = ConsoleLogger(
                service_name="html_processor", log_level=log_level
            )
            self.es_client_logger = ConsoleLogger(
                service_name="elasticsearch_client", log_level=log_level
            )

        # Initialize components with their specific loggers
        self.cosmos_client = CosmosAPIClient(cosmos_api_url, logger=self.cosmos_logger)
        self.downloader = AsyncHTMLDownloader(
            concurrency_limit=concurrency, logger=self.downloader_logger
        )
        self.processor = HTMLProcessor(logger=self.processor_logger)
        self.es_client = ElasticsearchClient(
            host=elasticsearch_host,
            index_name=elasticsearch_index,
            api_key=elasticsearch_api_key,
            logger=self.es_client_logger,
        )
        self.batch_size = batch_size

    async def run(self, collection_name: str, limit_batches: Optional[int] = None):
        """Run the complete pipeline for a collection.

        Args:
            collection_name: The collection name to process (e.g., "iau_minor_planet_system")
            limit_batches: Optional limit on the number of batches to process
        """
        start_time = time.time()
        total_urls = 0
        total_indexed = 0
        total_failed = 0
        batch_count = 0

        await self.logger.info(
            f"Starting pipeline for collection: {collection_name}",
            metadata={"collection": collection_name, "batch_size": self.batch_size},
        )

        try:
            # Process URL batches
            async for url_batch in self.cosmos_client.stream_collection_urls(
                collection_name, self.batch_size
            ):
                batch_count += 1
                batch_size = len(url_batch)
                total_urls += batch_size

                await self.logger.info(
                    f"Processing batch {batch_count} with {batch_size} URLs",
                    metadata={"batch": batch_count, "size": batch_size},
                )

                # 1. Download HTML for this batch concurrently
                batch_start = time.time()
                downloaded_batch = await self.downloader.download_batch(url_batch)
                download_time = time.time() - batch_start

                # 2. Process the downloaded content
                process_start = time.time()
                processed_batch = await self.processor.process_batch(downloaded_batch)
                process_time = time.time() - process_start

                # 3. Index the processed content
                index_start = time.time()
                index_result = await self.es_client.index_batch(processed_batch)
                index_time = time.time() - index_start

                # Track results
                batch_indexed = index_result.get("indexed", 0)
                batch_failed = index_result.get("failed", 0) + index_result.get(
                    "skipped", 0
                )
                total_indexed += batch_indexed
                total_failed += batch_failed

                await self.logger.info(
                    f"Batch {batch_count} results: {batch_indexed}/{batch_size} indexed successfully",
                    metadata={
                        "batch": batch_count,
                        "indexed": batch_indexed,
                        "failed": batch_failed,
                        "download_time": download_time,
                        "process_time": process_time,
                        "index_time": index_time,
                    },
                )

                # Stop if we've reached the batch limit
                if limit_batches and batch_count >= limit_batches:
                    await self.logger.info(
                        f"Reached batch limit of {limit_batches}, stopping",
                        metadata={"limit": limit_batches},
                    )
                    break

            # Final statistics
            elapsed = time.time() - start_time
            await self.logger.info(
                f"Pipeline completed: processed {total_urls} URLs in {batch_count} batches, "
                f"indexed {total_indexed} documents, {total_failed} failed",
                metadata={
                    "total_urls": total_urls,
                    "total_batches": batch_count,
                    "total_indexed": total_indexed,
                    "total_failed": total_failed,
                    "elapsed_time": elapsed,
                },
            )

        except Exception as e:
            await self.logger.error(
                f"Pipeline error: {str(e)}",
                error=str(e),
                metadata={"collection": collection_name},
            )
            raise

    async def close(self):
        """Close all connections and loggers."""
        # Close component connections and loggers
        await self.cosmos_client.close()
        await self.downloader.close()
        await self.processor.close()
        await self.es_client.close()

        # Close the component-specific loggers
        await self.cosmos_logger.close()
        await self.downloader_logger.close()
        await self.processor_logger.close()
        await self.es_client_logger.close()

        # Close the pipeline's own logger
        await self.logger.close()


async def main():
    """Run the pipeline based on command-line arguments and environment variables."""
    parser = argparse.ArgumentParser(description="COSMOS to Elasticsearch Pipeline")
    parser.add_argument(
        "--collection",
        "-c",
        default="iau_minor_planet_system",
        help="Collection name to process (default: iau_minor_planet_system)",
    )
    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=20,
        help="Number of URLs to process in each batch (default: 20)",
    )
    parser.add_argument(
        "--concurrency",
        "-cc",
        type=int,
        default=10,
        help="Maximum number of concurrent downloads (default: 10)",
    )
    parser.add_argument(
        "--limit",
        "-l",
        type=int,
        default=None,
        help="Limit the number of batches to process (default: no limit)",
    )
    parser.add_argument(
        "--log-type",
        "-lt",
        choices=["console", "elasticsearch"],
        default="console",
        help="Logger type to use (default: console)",
    )
    args = parser.parse_args()

    # Get configuration from environment variables
    es_host = os.environ.get(
        "ELASTIC_HOST",
        "https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
    )
    es_index = os.environ.get("ELASTIC_INDEX", "sde")
    es_api_key = os.environ.get("ELASTIC_API_KEY")
    cosmos_api_url = os.environ.get(
        "COSMOS_API_URL",
        "https://sde-indexing-helper.nasa-impact.net/candidate-urls-api",
    )

    # Set up pipeline logger
    if args.log_type == "elasticsearch" and es_host and es_api_key:
        print("Using Elasticsearch logger")
        logger = ElasticsearchLogger(
            host=es_host,
            index_prefix="sde_logs",
            service_name="pipeline_orchestrator",
            api_key=es_api_key,
        )
    else:
        print("Using Console logger")
        logger = ConsoleLogger(service_name="pipeline_orchestrator", log_level="INFO")

    # Initialize pipeline
    pipeline = Pipeline(
        cosmos_api_url=cosmos_api_url,
        elasticsearch_host=es_host,
        elasticsearch_index=es_index,
        elasticsearch_api_key=es_api_key,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        logger=logger,
    )

    try:
        # Run the pipeline
        print(f"Starting pipeline for collection: {args.collection}")
        await pipeline.run(args.collection, limit_batches=args.limit)
        print("Pipeline completed successfully")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise

    finally:
        # Close all connections
        await pipeline.close()
        print("All connections closed")


if __name__ == "__main__":
    asyncio.run(main())
