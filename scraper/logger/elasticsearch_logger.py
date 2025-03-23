import logging
from typing import Dict, Any, Optional
from datetime import datetime
from elasticsearch import AsyncElasticsearch
from .logger_interface import LoggerInterface


class ElasticsearchLogger(LoggerInterface):
    """Logger implementation that sends logs to Elasticsearch."""

    def __init__(
        self,
        host: str,
        index_prefix: str,
        service_name: str,
        component: str = "general",
        api_key: Optional[str] = None,
    ):
        """Initialize the Elasticsearch logger.

        Args:
            host: Elasticsearch host URL
            index_prefix: Prefix for the log indices
            service_name: Name of the service (used in the index name)
            api_key: API key for authentication
        """
        self.host = host
        self.index_prefix = index_prefix
        self.service_name = service_name
        self.component = component
        self.api_key = api_key
        self.client = None

        # Set up standard Python logger as a fallback
        self.logger = logging.getLogger(f"es_logger.{service_name}")
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    @property
    def index_name(self) -> str:
        """Get the index name for this service's logs."""
        return f"{self.index_prefix}_{self.service_name}_logs"

    async def _ensure_client(self):
        """Ensure that an Elasticsearch client exists."""
        if self.client is None:
            client_options = {
                "hosts": [self.host],
                "retry_on_timeout": True,
                "max_retries": 3,
                "timeout": 30,
            }

            if self.api_key:
                client_options["api_key"] = self.api_key

            self.client = AsyncElasticsearch(**client_options)

    async def ensure_index_exists(self):
        """Check if the log index exists and create it if it doesn't."""
        await self._ensure_client()

        exists = await self.client.indices.exists(index=self.index_name)
        if exists:
            self.logger.info(f"Log index {self.index_name} already exists")
            return

        mappings = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "service": {"type": "keyword"},
                    "level": {"type": "keyword"},
                    "message": {"type": "text", "analyzer": "standard"},
                    "url": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "error": {"type": "text", "analyzer": "standard"},
                    "metadata": {"type": "object", "enabled": True},
                }
            }
        }

        try:
            await self.client.indices.create(index=self.index_name, body=mappings)
            self.logger.info(f"Successfully created log index {self.index_name}")
        except Exception as e:
            self.logger.error(f"Failed to create log index: {str(e)}")
            raise

    async def log(
        self,
        level: str,
        message: str,
        url: Optional[str] = None,
        status: Optional[str] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Log a message to Elasticsearch.

        Args:
            level: Log level (info, warning, error)
            message: Main log message
            url: Optional URL related to the log
            status: Optional status code or message
            error: Optional error details
            metadata: Optional additional metadata
        """
        await self._ensure_client()
        await self.ensure_index_exists()

        # Standard Python logging as fallback
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(f"{message} - URL: {url} - Status: {status} - Error: {error}")

        # Create log document
        log_doc = {
            "timestamp": datetime.utcnow(),
            "service": self.service_name,
            "level": level,
            "message": message,
        }

        # Add optional fields if provided
        if url:
            log_doc["url"] = url
        if status:
            log_doc["status"] = status
        if error:
            log_doc["error"] = error
        if metadata:
            log_doc["metadata"] = metadata

        try:
            # Index the log
            await self.client.index(index=self.index_name, body=log_doc)
        except Exception as e:
            # If ES logging fails, at least we have the Python logger as backup
            self.logger.error(f"Failed to log to Elasticsearch: {str(e)}")

    async def info(self, message: str, **kwargs):
        """Log an info-level message."""
        await self.log("info", message, **kwargs)

    async def warning(self, message: str, **kwargs):
        """Log a warning-level message."""
        await self.log("warning", message, **kwargs)

    async def error(self, message: str, **kwargs):
        """Log an error-level message."""
        await self.log("error", message, **kwargs)

    async def log_failed_download(
        self,
        url_metadata: Dict[str, Any],
        error: str,
        status_code: Optional[int] = None,
    ):
        """Specialized method to log failed downloads."""
        await self.error(
            message=f"Failed to download content",
            url=url_metadata.get("url", "unknown"),
            status=str(status_code) if status_code else "unknown",
            error=error,
            metadata=url_metadata,
        )

    async def close(self):
        """Close the Elasticsearch client."""
        if self.client is not None:
            await self.client.close()
            self.client = None
