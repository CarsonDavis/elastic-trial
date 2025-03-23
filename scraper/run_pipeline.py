#!/usr/bin/env python3
"""
Full Collection Pipeline Script

This script processes an entire collection of URLs from the COSMOS API,
downloading their content and indexing them in Elasticsearch.
It includes progress tracking and statistics reporting.
"""

import asyncio
import os
import time
import argparse
import logging
import sys
from typing import Dict, Any, Optional

# Import the pipeline and logger classes
from pipeline import Pipeline
from logger.console_logger import ConsoleLogger
from logger.elasticsearch_logger import ElasticsearchLogger


class ProgressTracker:
    """Tracks and displays progress of the pipeline processing."""

    def __init__(self, update_interval: int = 10):
        """
        Initialize the progress tracker.

        Args:
            update_interval: How often to print updates (in seconds)
        """
        self.start_time = time.time()
        self.update_interval = update_interval
        self.last_update = self.start_time
        self.batch_count = 0
        self.total_urls = 0
        self.indexed_count = 0
        self.failed_count = 0
        self.estimated_total = None

    def update(
        self,
        batch_size: int,
        indexed: int,
        failed: int,
        estimated_total: Optional[int] = None,
    ):
        """
        Update the progress statistics.

        Args:
            batch_size: Size of the current batch
            indexed: Number of documents successfully indexed in this batch
            failed: Number of documents that failed in this batch
            estimated_total: Optional estimated total number of URLs in the collection
        """
        self.batch_count += 1
        self.total_urls += batch_size
        self.indexed_count += indexed
        self.failed_count += failed

        if estimated_total is not None:
            self.estimated_total = estimated_total

        # Check if it's time to print an update
        current_time = time.time()
        if current_time - self.last_update >= self.update_interval:
            self.print_progress()
            self.last_update = current_time

    def print_progress(self):
        """Print the current progress statistics."""
        elapsed = time.time() - self.start_time
        urls_per_second = self.total_urls / elapsed if elapsed > 0 else 0

        print(f"\n----- Progress Update -----")
        print(f"Processed batches: {self.batch_count}")
        print(f"Total URLs: {self.total_urls}")
        print(f"Successfully indexed: {self.indexed_count}")
        print(f"Failed: {self.failed_count}")
        print(f"Processing rate: {urls_per_second:.1f} URLs/second")

        if self.estimated_total:
            percent_complete = (self.total_urls / self.estimated_total) * 100
            print(
                f"Progress: {percent_complete:.1f}% ({self.total_urls}/{self.estimated_total})"
            )

            # Estimate time remaining
            if urls_per_second > 0:
                remaining_urls = self.estimated_total - self.total_urls
                remaining_time = remaining_urls / urls_per_second
                remaining_hours = int(remaining_time // 3600)
                remaining_minutes = int((remaining_time % 3600) // 60)
                print(
                    f"Estimated time remaining: {remaining_hours}h {remaining_minutes}m"
                )

        print(f"Elapsed time: {self._format_time(elapsed)}")
        print("---------------------------")

    def print_final_stats(self):
        """Print the final statistics after processing is complete."""
        elapsed = time.time() - self.start_time

        print("\n" + "=" * 50)
        print("FINAL STATISTICS")
        print("=" * 50)
        print(f"Total batches processed: {self.batch_count}")
        print(f"Total URLs processed: {self.total_urls}")
        print(f"Successfully indexed: {self.indexed_count}")
        print(f"Failed: {self.failed_count}")

        # Calculate success rate with division by zero protection
        if self.total_urls > 0:
            success_rate = (self.indexed_count / self.total_urls) * 100
            print(f"Success rate: {success_rate:.1f}%")
        else:
            print("Success rate: N/A (no URLs processed)")

        print(f"Total processing time: {self._format_time(elapsed)}")

        # Calculate processing speed with division by zero protection
        if elapsed > 0:
            print(
                f"Average processing speed: {self.total_urls / elapsed:.1f} URLs/second"
            )
        else:
            print("Average processing speed: N/A (elapsed time too short)")

        print("=" * 50)

    def _format_time(self, seconds: float) -> str:
        """Format seconds into a readable time string."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours}h {minutes}m {secs}s"


async def process_collection(
    collection_name: str = "iau_minor_planet_system",
    batch_size: int = 20,
    concurrency: int = 10,
    log_type: str = "console",
    update_interval: int = 10,
):
    """
    Process an entire collection from the COSMOS API.

    Args:
        collection_name: The COSMOS API collection to process
        batch_size: Number of URLs to process in each batch
        concurrency: Maximum number of concurrent downloads
        log_type: Type of logger to use ("console" or "elasticsearch")
        update_interval: How often to print progress updates (in seconds)
    """
    print("\n" + "=" * 80)
    print(f"FULL COLLECTION PROCESSING - Collection: {collection_name}")
    print("=" * 80)

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

    # Check for required API key if using Elasticsearch
    if log_type == "elasticsearch" and not es_api_key:
        print("WARNING: ELASTIC_API_KEY environment variable not set.")
        print("Switching to console logging.")
        log_type = "console"

    # Confirm with user before proceeding
    print(f"\nYou are about to process the ENTIRE '{collection_name}' collection.")
    print(f"This will download and index ALL URLs in batches of {batch_size}.")
    print(
        "Depending on the collection size, this could take a long time and use significant resources."
    )

    confirmation = input("\nDo you want to continue? (yes/no): ").strip().lower()
    if confirmation != "yes":
        print("Operation cancelled by user.")
        return

    # Set up appropriate logger
    if log_type == "elasticsearch":
        print(f"\nUsing Elasticsearch logger with host: {es_host}")
        logger = ElasticsearchLogger(
            host=es_host,
            index_prefix="sde_logs",
            service_name="pipeline_full",
            api_key=es_api_key,
        )
    else:
        print("\nUsing Console logger")
        logger = ConsoleLogger(service_name="pipeline_full", log_level="INFO")

    # Initialize progress tracker
    progress = ProgressTracker(update_interval=update_interval)

    try:
        # Initialize the pipeline
        print("\nInitializing pipeline components...")
        pipeline = Pipeline(
            cosmos_api_url=cosmos_api_url,
            elasticsearch_host=es_host,
            elasticsearch_index=es_index,
            elasticsearch_api_key=es_api_key,
            concurrency=concurrency,
            batch_size=batch_size,
            logger=logger,
        )

        # Create a subclass of Pipeline to track progress
        class TrackingPipeline(Pipeline):
            async def run(
                self, collection_name: str, limit_batches: Optional[int] = None
            ):
                start_time = time.time()
                total_urls = 0
                total_indexed = 0
                total_failed = 0
                batch_count = 0

                await self.logger.info(
                    f"Starting pipeline for collection: {collection_name}",
                    metadata={
                        "collection": collection_name,
                        "batch_size": self.batch_size,
                    },
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
                        downloaded_batch = await self.downloader.download_batch(
                            url_batch
                        )
                        download_time = time.time() - batch_start

                        # 2. Process the downloaded content
                        process_start = time.time()
                        processed_batch = await self.processor.process_batch(
                            downloaded_batch
                        )
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

                        # Update progress tracker
                        progress.update(
                            batch_size=batch_size,
                            indexed=batch_indexed,
                            failed=batch_failed,
                        )

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

        # Create our tracking pipeline by copying the original pipeline's components
        tracking_pipeline = TrackingPipeline(
            cosmos_api_url=cosmos_api_url,
            elasticsearch_host=es_host,
            elasticsearch_index=es_index,
            elasticsearch_api_key=es_api_key,
            concurrency=concurrency,
            batch_size=batch_size,
            logger=logger,
        )
        tracking_pipeline.cosmos_client = pipeline.cosmos_client
        tracking_pipeline.downloader = pipeline.downloader
        tracking_pipeline.processor = pipeline.processor
        tracking_pipeline.es_client = pipeline.es_client

        # Run the tracking pipeline with no batch limit
        print(f"\nProcessing collection '{collection_name}'...")
        print(
            f"Using batch size of {batch_size} URLs with concurrency limit of {concurrency}"
        )
        print(f"Progress updates will be shown every {update_interval} seconds")
        print("\nStarting processing... Press Ctrl+C to stop safely.")

        await tracking_pipeline.run(collection_name=collection_name)

        # Show final statistics
        progress.print_final_stats()

    except KeyboardInterrupt:
        print("\n\nProcessing interrupted by user. Cleaning up...")
        progress.print_final_stats()

    except Exception as e:
        print(f"\n\nERROR: Processing failed with exception: {str(e)}")
        progress.print_final_stats()
        raise

    finally:
        # Always close connections
        if "pipeline" in locals():
            print("\nClosing all connections...")
            await pipeline.close()

        print("\nProcessing completed. Check the logs for detailed information.")

        # If using Elasticsearch logging, output information about where to find logs
        if log_type == "elasticsearch":
            print("\nElasticsearch Log Indices:")
            print("- sde_logs_pipeline_full_logs (Pipeline logs)")
            print("- sde_logs_cosmos_api_logs (COSMOS API logs)")
            print("- sde_logs_html_downloader_logs (HTML downloader logs)")
            print("- sde_logs_html_processor_logs (HTML processor logs)")
            print("- sde_logs_elasticsearch_client_logs (Elasticsearch client logs)")

        print("\n" + "=" * 80)


async def main():
    """Parse command line arguments and process the collection."""
    parser = argparse.ArgumentParser(description="Process a full COSMOS API collection")
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
        "--log-type",
        "-lt",
        choices=["console", "elasticsearch"],
        default="elasticsearch",
        help="Logger type to use (default: console)",
    )
    parser.add_argument(
        "--update-interval",
        "-u",
        type=int,
        default=10,
        help="Progress update interval in seconds (default: 10)",
    )

    args = parser.parse_args()

    await process_collection(
        collection_name=args.collection,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        log_type=args.log_type,
        update_interval=args.update_interval,
    )


if __name__ == "__main__":
    print("COSMOS Full Collection Pipeline")
    asyncio.run(main())
