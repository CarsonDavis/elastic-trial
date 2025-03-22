import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
from logger.logger_interface import LoggerInterface
from logger.console_logger import ConsoleLogger
import hashlib


class ElasticsearchClient:
    """Handles interaction with Elasticsearch for indexing content using the official client."""

    def __init__(
        self,
        host: str,
        index_name: str,
        api_key: Optional[str] = None,
        max_retries: int = 3,
        retry_on_timeout: bool = True,
        timeout: int = 30,
        logger: Optional[LoggerInterface] = None,
    ):
        """Initialize the Elasticsearch client.

        Args:
            host: Elasticsearch host URL (e.g., https://my-elasticsearch.cloud:443)
            index_name: Name of the index to use
            api_key: API key for authentication (id:api_key format)
            max_retries: Maximum number of retry attempts for failed operations
            retry_on_timeout: Whether to retry on timeout
            timeout: Request timeout in seconds
            logger: External logger implementation (must implement LoggerInterface).
                   If None, a ConsoleLogger will be created automatically.
        """
        self.host = host
        self.index_name = index_name
        self.api_key = api_key
        self.client = None

        # Client options
        self.client_options = {
            "hosts": [self.host],
            "retry_on_timeout": retry_on_timeout,
            "max_retries": max_retries,
            "timeout": timeout,
        }

        if api_key:
            self.client_options["api_key"] = api_key

        # Create default ConsoleLogger if no logger is provided
        self.logger = (
            logger
            if logger is not None
            else ConsoleLogger(service_name="elastic_upload")
        )

    async def _ensure_client(self):
        """Ensure that an Elasticsearch client exists."""
        if self.client is None:
            self.client = AsyncElasticsearch(**self.client_options)

    def _get_document_id(self, doc: Dict[str, Any]) -> str:
        """Generate a unique document ID based on the URL.

        This ensures that documents with the same URL will overwrite each other,
        keeping only the most recent version.
        """
        url = doc.get("url", "")
        if not url:
            # Generate a random ID if URL is missing
            return hashlib.md5(str(datetime.utcnow().timestamp()).encode()).hexdigest()

        # Use the URL directly as the document ID
        # We URL-encode or hash it to avoid special characters issues
        return hashlib.md5(url.encode()).hexdigest()

    def _prepare_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a document for indexing in Elasticsearch."""
        # Extract key fields from the document
        url = doc.get("url", "")
        title = doc.get("title", "")
        document_type = doc.get("document_type", "")
        html_content = doc.get("html", "")
        tree_root = doc.get("tree_root", "")

        # Get text content if processed by HTMLProcessor
        text_content = doc.get("text_content", "")

        # Create a document structure optimized for search
        prepared_doc = {
            # Core metadata
            "url": url,
            "title": title,
            "document_type": document_type,
            "tree_root": tree_root,
            "file_extension": doc.get("file_extension", ""),
            # Content
            "html_content": html_content,
            "text_content": text_content,
            # Processing metadata
            "indexed_at": datetime.utcnow(),
            "last_scraped": datetime.utcnow(),
            "processing_status": doc.get("processing_status", "raw"),
            # Original metadata preserved, excluding download-related fields
            "original_metadata": {
                k: v
                for k, v in doc.items()
                if k
                not in [
                    "html",
                    "html_content",
                    "text_content",
                    "download_status",
                    "status_code",
                    "download_error",
                    "download_time",
                ]
            },
        }

        return prepared_doc

    async def ensure_index_exists(self):
        """Check if the index exists and create it if it doesn't."""
        await self._ensure_client()

        exists = await self.client.indices.exists(index=self.index_name)
        if exists:
            await self.logger.info(f"Index {self.index_name} already exists")
            return

        # Define index mappings
        mappings = {
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},  # Keyword for exact matches
                    "title": {"type": "text", "analyzer": "standard"},
                    "document_type": {"type": "keyword"},
                    "tree_root": {"type": "keyword"},
                    "file_extension": {"type": "keyword"},
                    "html_content": {"type": "text", "analyzer": "standard"},
                    "text_content": {"type": "text", "analyzer": "standard"},
                    "indexed_at": {"type": "date"},
                    "last_scraped": {"type": "date"},
                    "processing_status": {"type": "keyword"},
                }
            }
        }

        try:
            await self.client.indices.create(index=self.index_name, body=mappings)
            await self.logger.info(f"Successfully created index {self.index_name}")
        except Exception as e:
            await self.logger.error(f"Failed to create index: {str(e)}", error=str(e))
            raise

    async def _generate_bulk_actions(self, documents):
        """Generate actions for bulk indexing, filtering out failed downloads."""
        successful_docs = 0
        skipped_docs = 0

        for doc in documents:
            url = doc.get("url", "unknown")
            download_status = doc.get("download_status", "unknown")

            # Only index documents that were successfully downloaded and have HTML content
            if download_status == "success" and doc.get("html"):
                successful_docs += 1
                # Generate a document ID based on URL to ensure uniqueness
                doc_id = self._get_document_id(doc)

                yield {
                    "_index": self.index_name,
                    "_id": doc_id,  # Using URL-based ID for uniqueness
                    "_source": self._prepare_document(doc),
                }
            else:
                skipped_docs += 1
                # Log skipped documents
                error_reason = doc.get("download_error", "No HTML content")
                await self.logger.warning(
                    f"Skipping document indexing for {url} due to status {download_status}",
                    url=url,
                    status=download_status,
                    error=error_reason,
                    metadata={"skipped": True},
                )

        await self.logger.info(
            f"Prepared {successful_docs} documents for indexing, skipped {skipped_docs} failed documents",
            metadata={"successful": successful_docs, "skipped": skipped_docs},
        )

    async def index_batch(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Index a batch of documents in Elasticsearch using the bulk API."""
        if not documents:
            await self.logger.warning("Empty document batch provided for indexing")
            return {"indexed": 0, "failed": 0, "skipped": 0, "errors": []}

        await self._ensure_client()
        await self.ensure_index_exists()

        # Pre-filter to count successful and failed documents
        successful_documents = [
            doc
            for doc in documents
            if doc.get("download_status") == "success" and doc.get("html")
        ]
        skipped_documents = [
            doc
            for doc in documents
            if doc.get("download_status") != "success" or not doc.get("html")
        ]

        await self.logger.info(
            f"Processing batch with {len(documents)} total documents: "
            f"{len(successful_documents)} successful, {len(skipped_documents)} failed/skipped",
            metadata={
                "total_documents": len(documents),
                "successful_documents": len(successful_documents),
                "skipped_documents": len(skipped_documents),
            },
        )

        # If no successful documents, return early
        if not successful_documents:
            await self.logger.warning(
                "No documents to index after filtering out failed downloads",
                metadata={"skipped_documents": len(skipped_documents)},
            )
            return {
                "indexed": 0,
                "failed": 0,
                "skipped": len(skipped_documents),
                "errors": [],
            }

        try:
            # Use the async_bulk helper with generator that filters out failed downloads
            success, errors = await async_bulk(
                client=self.client,
                actions=self._generate_bulk_actions(documents),
                stats_only=False,
                raise_on_error=False,
                max_retries=3,
            )

            # Log errors if any
            if errors:
                for error in errors[:5]:  # Log only the first few errors
                    await self.logger.error(
                        f"Indexing error: {error}", error=str(error)
                    )

            await self.logger.info(
                f"Indexing completed: {success} documents indexed successfully, "
                f"{len(errors)} indexing errors, {len(skipped_documents)} skipped due to download failure",
                metadata={
                    "indexed": success,
                    "indexing_errors": len(errors),
                    "skipped": len(skipped_documents),
                },
            )

            return {
                "indexed": success,
                "failed": len(errors),
                "skipped": len(skipped_documents),
                "total": len(documents),
                "errors": errors,
            }

        except Exception as e:
            await self.logger.error(
                f"Error during bulk indexing: {str(e)}", error=str(e)
            )
            raise

    async def close(self):
        """Close the Elasticsearch client and logger."""
        if self.client is not None:
            await self.client.close()
            self.client = None

        # Close the logger
        await self.logger.close()


# Example usage
async def main():
    import os

    # Get API key from environment variable
    es_api_key = os.environ.get("ELASTIC_API_KEY")
    if not es_api_key:
        print("Warning: ELASTIC_API_KEY environment variable not set. Using demo mode.")
        es_api_key = "demo_key"

    # Sample documents (normally these would come from the HTML downloader)
    sample_docs = [
        {
            "url": "https://data.minorplanetcenter.net/",
            "title": "The International Astronomical Union Minor Planet Center",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
            "html": "<html><head><title>MPC</title></head><body>Sample content</body></html>",
            "download_status": "success",
            "status_code": 200,
            "download_time": 0.45,
        },
        {
            "url": "https://example.com/failed-page",
            "title": "Failed Download Example",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
            "html": None,
            "download_status": "error",
            "status_code": 404,
            "download_error": "Page not found",
        },
    ]

    # Initialize the client (will use ConsoleLogger by default)
    client = ElasticsearchClient(
        host="https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
        index_name="sde_index_custom_scraper",
        api_key=es_api_key,
    )

    try:
        # Index the sample documents
        print("\nStarting indexing process...")
        result = await client.index_batch(sample_docs)

        # Print results
        print("\nIndexing Summary:")
        print(f"- Total documents processed: {result['total']}")
        print(f"- Successfully indexed: {result['indexed']}")
        print(f"- Failed during indexing: {result['failed']}")
        print(f"- Skipped due to download failure: {result['skipped']}")

        if result.get("errors"):
            print(f"\nFirst indexing error: {result['errors'][0]}")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
