from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class LoggerInterface(ABC):
    """Abstract interface for different logger implementations."""

    @abstractmethod
    async def info(self, message: str, **kwargs):
        """Log an information message."""
        pass

    @abstractmethod
    async def warning(self, message: str, **kwargs):
        """Log a warning message."""
        pass

    @abstractmethod
    async def error(self, message: str, **kwargs):
        """Log an error message."""
        pass

    @abstractmethod
    async def log_failed_download(
        self,
        url_metadata: Dict[str, Any],
        error: str,
        status_code: Optional[int] = None,
    ):
        """Log a failed download with metadata."""
        pass

    @abstractmethod
    async def close(self):
        """Close the logger and any connections."""
        pass
