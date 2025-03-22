import asyncio
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


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
    ):
        """Initialize the Elasticsearch client.

        Args:
            host: Elasticsearch host URL (e.g., https://my-elasticsearch.cloud:443)
            index_name: Name of the index to use
            api_key: API key for authentication (id:api_key format)
            max_retries: Maximum number of retry attempts for failed operations
            retry_on_timeout: Whether to retry on timeout
            timeout: Request timeout in seconds
        """
        self.host = host
        self.index_name = index_name
        self.api_key = api_key
        self.client = None
        self.logger = logging.getLogger(__name__)

        # Client options
        self.client_options = {
            "hosts": [self.host],
            "retry_on_timeout": retry_on_timeout,
            "max_retries": max_retries,
            "timeout": timeout,
        }

        if api_key:
            self.client_options["api_key"] = api_key

        # Configure logging
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    async def _ensure_client(self):
        """Ensure that an Elasticsearch client exists."""
        if self.client is None:
            self.client = AsyncElasticsearch(**self.client_options)

    def _prepare_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare a document for indexing in Elasticsearch."""
        # Extract key fields from the document
        url = doc.get("url", "")
        title = doc.get("title", "")
        document_type = doc.get("document_type", "")
        html_content = doc.get("html", "")
        tree_root = doc.get("tree_root", "")
        download_status = doc.get("download_status", "unknown")

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
            # Download metadata
            "download_status": download_status,
            "status_code": doc.get("status_code"),
            "download_error": doc.get("download_error"),
            "download_time": doc.get("download_time"),
            # Processing metadata
            "indexed_at": datetime.utcnow(),  # The client handles date serialization
            "processing_status": "raw",
            # Original metadata preserved
            "original_metadata": {
                k: v
                for k, v in doc.items()
                if k
                not in [
                    "html",
                    "html_content",
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
            self.logger.info(f"Index {self.index_name} already exists")
            return

        # Define index mappings
        mappings = {
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "title": {"type": "text", "analyzer": "standard"},
                    "document_type": {"type": "keyword"},
                    "tree_root": {"type": "keyword"},
                    "file_extension": {"type": "keyword"},
                    "html_content": {"type": "text", "analyzer": "standard"},
                    "download_status": {"type": "keyword"},
                    "status_code": {"type": "integer"},
                    "download_error": {"type": "text", "analyzer": "standard"},
                    "download_time": {"type": "float"},
                    "indexed_at": {"type": "date"},
                    "processing_status": {"type": "keyword"},
                }
            },
            "settings": {"number_of_shards": 1, "number_of_replicas": 1},
        }

        try:
            await self.client.indices.create(index=self.index_name, body=mappings)
            self.logger.info(f"Successfully created index {self.index_name}")
        except Exception as e:
            self.logger.error(f"Failed to create index: {str(e)}")
            raise

    async def _generate_bulk_actions(self, documents):
        """Generate actions for bulk indexing."""
        for doc in documents:
            if doc.get("download_status") == "success" and doc.get("html"):
                yield {
                    "_index": self.index_name,
                    "_source": self._prepare_document(doc),
                }
            else:
                self.logger.warning(
                    f"Skipping document with URL {doc.get('url')} due to download status {doc.get('download_status')}"
                )

    async def index_batch(self, documents: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Index a batch of documents in Elasticsearch using the bulk API."""
        if not documents:
            self.logger.warning("Empty document batch provided for indexing")
            return {"indexed": 0, "failed": 0, "errors": []}

        await self._ensure_client()
        await self.ensure_index_exists()

        self.logger.info(f"Indexing batch of {len(documents)} documents")

        try:
            # Use the async_bulk helper
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
                    self.logger.error(f"Indexing error: {error}")

            self.logger.info(
                f"Indexed {success} documents successfully, {len(errors)} failed"
            )

            return {
                "indexed": success,
                "failed": len(errors),
                "total": success + len(errors),
                "errors": errors,
            }

        except Exception as e:
            self.logger.error(f"Error during bulk indexing: {str(e)}", exc_info=True)
            raise

    async def close(self):
        """Close the Elasticsearch client."""
        if self.client is not None:
            await self.client.close()
            self.client = None


# Example usage
async def main():
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

    # Initialize the client
    client = ElasticsearchClient(
        host="https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
        index_name="sde_index_custom_scraper",
        api_key="your_api_key_here",  # Replace with actual API key
    )

    try:
        # Index the sample documents
        result = await client.index_batch(sample_docs)

        # Print results
        print(
            f"Indexing completed: {result['indexed']} indexed, {result.get('failed', 0)} failed"
        )
        if result.get("errors"):
            print(f"First error: {result['errors'][0]}")

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
