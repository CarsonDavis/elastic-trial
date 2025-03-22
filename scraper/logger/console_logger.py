import logging
import json
from typing import Dict, Any, Optional
from .logger_interface import LoggerInterface


class ConsoleLogger(LoggerInterface):
    """Logger implementation that outputs to the console."""

    def __init__(self, service_name: str, log_level: str = "INFO"):
        """Initialize the console logger.

        Args:
            service_name: Name of the service (used in log formatting)
            log_level: Minimum log level to display (DEBUG, INFO, WARNING, ERROR)
        """
        self.service_name = service_name

        # Set up Python logger
        self.logger = logging.getLogger(f"console_logger.{service_name}")

        # Clear any existing handlers
        if self.logger.handlers:
            self.logger.handlers.clear()

        # Add console handler with detailed formatting
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Set log level
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)

    def _format_metadata(self, **kwargs) -> str:
        """Format additional metadata for logging."""
        metadata = {}

        # Extract common fields
        for key in ["url", "status", "error"]:
            if key in kwargs and kwargs[key]:
                metadata[key] = kwargs[key]

        # Add any other metadata
        if "metadata" in kwargs and kwargs["metadata"]:
            metadata["metadata"] = kwargs["metadata"]

        # Format as JSON if there's metadata
        if metadata:
            return f" | {json.dumps(metadata)}"
        return ""

    async def info(self, message: str, **kwargs):
        """Log an info-level message."""
        self.logger.info(f"{message}{self._format_metadata(**kwargs)}")

    async def warning(self, message: str, **kwargs):
        """Log a warning-level message."""
        self.logger.warning(f"{message}{self._format_metadata(**kwargs)}")

    async def error(self, message: str, **kwargs):
        """Log an error-level message."""
        self.logger.error(f"{message}{self._format_metadata(**kwargs)}")

    async def log_failed_download(
        self,
        url_metadata: Dict[str, Any],
        error: str,
        status_code: Optional[int] = None,
    ):
        """Specialized method to log failed downloads."""
        message = (
            f"Failed to download content from {url_metadata.get('url', 'unknown')}"
        )
        status = f"Status: {status_code}" if status_code else "Status: unknown"

        self.logger.error(
            f"{message} | {status} | Error: {error} | "
            f"Metadata: {json.dumps({k: v for k, v in url_metadata.items() if k != 'html'})}"
        )

    async def close(self):
        """Close the logger (no-op for console logger)."""
        pass  # Nothing to close for console logger
