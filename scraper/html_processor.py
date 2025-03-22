import asyncio
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional, Callable
from logger.logger_interface import LoggerInterface
from logger.console_logger import ConsoleLogger


class HTMLProcessor:
    """Processes downloaded HTML content and prepares it for indexing."""

    def __init__(
        self, plugins: List[Callable] = None, logger: Optional[LoggerInterface] = None
    ):
        """Initialize the HTML processor.

        Args:
            plugins: List of plugin functions that will process the HTML
            logger: External logger implementation (must implement LoggerInterface).
                   If None, a ConsoleLogger will be created automatically.
        """
        self.plugins = plugins or []

        # Create default ConsoleLogger if no logger is provided
        self.logger = (
            logger
            if logger is not None
            else ConsoleLogger(service_name="html_processor")
        )

    async def process_document(self, document: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single document's HTML content.

        Args:
            document: Document dictionary from the HTML downloader with URL metadata and HTML

        Returns:
            Processed document with any additional extracted information
        """
        url = document.get("url", "unknown")
        html = document.get("html")
        download_status = document.get("download_status")

        # Skip processing if the download failed
        if download_status != "success" or not html:
            await self.logger.warning(
                f"Skipping processing for {url} due to failed download",
                url=url,
                status=download_status,
            )
            return document

        try:
            # Create a copy of the document to avoid modifying the original
            processed_doc = document.copy()

            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")

            # Extract plain text content
            text_content = soup.get_text(separator=" ", strip=True)
            processed_doc["text_content"] = text_content

            # Extract metadata from HTML
            # Title from HTML if not already in metadata
            if not processed_doc.get("title") and soup.title:
                processed_doc["title"] = (
                    soup.title.string.strip() if soup.title.string else ""
                )

            # Extract meta tags
            meta_tags = {}
            for meta in soup.find_all("meta"):
                name = meta.get("name") or meta.get("property")
                content = meta.get("content")
                if name and content:
                    meta_tags[name] = content

            if meta_tags:
                processed_doc["meta_tags"] = meta_tags

            # Run any custom plugins
            for plugin in self.plugins:
                try:
                    plugin_result = plugin(soup, processed_doc)
                    if plugin_result:
                        processed_doc.update(plugin_result)
                except Exception as plugin_error:
                    await self.logger.error(
                        f"Plugin error processing {url}: {str(plugin_error)}",
                        url=url,
                        error=str(plugin_error),
                    )

            # Set processing status
            processed_doc["processing_status"] = "processed"

            await self.logger.info(
                f"Successfully processed HTML for {url}",
                url=url,
                metadata={"text_length": len(text_content) if text_content else 0},
            )

            return processed_doc

        except Exception as e:
            await self.logger.error(
                f"Error processing HTML for {url}: {str(e)}",
                url=url,
                error=str(e),
            )

            # Return the original document with a processing error flag
            document["processing_status"] = "error"
            document["processing_error"] = str(e)
            return document

    async def process_batch(
        self, documents: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Process a batch of HTML documents concurrently.

        Args:
            documents: List of document dictionaries from the HTML downloader

        Returns:
            List of processed documents
        """
        if not documents:
            await self.logger.warning("Empty document batch provided for processing")
            return []

        await self.logger.info(
            f"Processing batch of {len(documents)} documents",
            metadata={"batch_size": len(documents)},
        )

        # Process documents concurrently
        tasks = [self.process_document(doc) for doc in documents]
        processed_docs = await asyncio.gather(*tasks, return_exceptions=True)

        # Handle any exceptions
        result_docs = []
        for i, doc in enumerate(processed_docs):
            if isinstance(doc, Exception):
                error_msg = f"Unhandled exception during processing: {str(doc)}"
                await self.logger.error(
                    error_msg, url=documents[i].get("url", "unknown"), error=str(doc)
                )

                # Create an error result using the original document
                error_doc = documents[i].copy()
                error_doc["processing_status"] = "error"
                error_doc["processing_error"] = str(doc)
                result_docs.append(error_doc)
            else:
                result_docs.append(doc)

        # Count successful and failed documents
        successful = sum(
            1 for doc in result_docs if doc.get("processing_status") == "processed"
        )
        failed = len(result_docs) - successful

        await self.logger.info(
            f"Completed batch processing: {successful}/{len(documents)} successful",
            metadata={
                "successful": successful,
                "failed": failed,
                "total": len(documents),
            },
        )

        return result_docs

    async def close(self):
        """Close the processor and logger."""
        await self.logger.close()


# Example plugin functions
def extract_links_plugin(
    soup: BeautifulSoup, document: Dict[str, Any]
) -> Dict[str, Any]:
    """Plugin to extract all links from the document."""
    links = [a.get("href") for a in soup.find_all("a") if a.get("href")]
    return {"links": links}


def extract_images_plugin(
    soup: BeautifulSoup, document: Dict[str, Any]
) -> Dict[str, Any]:
    """Plugin to extract image information from the document."""
    images = [
        {
            "src": img.get("src"),
            "alt": img.get("alt", ""),
            "width": img.get("width", ""),
            "height": img.get("height", ""),
        }
        for img in soup.find_all("img")
        if img.get("src")
    ]
    return {"images": images}


# Example usage
async def main():
    import asyncio
    from html_downloader import AsyncHTMLDownloader

    # Create sample URL metadata
    sample_urls = [
        {
            "url": "https://data.minorplanetcenter.net/",
            "title": "The International Astronomical Union Minor Planet Center",
            "document_type": "Documentation",
            "file_extension": "html",
            "tree_root": "/Planetary Science/IAU Minor Planet System/",
            "tdamm_tag": [],
        }
    ]

    # Set up the components
    downloader = AsyncHTMLDownloader()
    processor = HTMLProcessor(plugins=[extract_links_plugin, extract_images_plugin])

    try:
        # Download HTML for the sample URLs
        downloaded_batch = await downloader.download_batch(sample_urls)

        # Process the downloaded content
        processed_batch = await processor.process_batch(downloaded_batch)

        # Print results
        for doc in processed_batch:
            print(f"\nProcessed document: {doc['url']}")
            print(f"Title: {doc.get('title', 'N/A')}")
            print(doc.get("text_content", "no text extracted"))
            print(f"Processing status: {doc.get('processing_status', 'N/A')}")

            if doc.get("links"):
                print(f"Found {len(doc['links'])} links")

            if doc.get("images"):
                print(f"Found {len(doc['images'])} images")

            if doc.get("processing_error"):
                print(f"Error: {doc['processing_error']}")

    finally:
        await downloader.close()
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())
