# COSMOS to Elasticsearch Pipeline Architecture (Async Streaming Stack)

## Architecture Overview

This document outlines the architecture for a modular, extensible, and parallelized pipeline that:
1. Streams URL metadata from the COSMOS API in memory-efficient batches
2. Downloads HTML content from these URLs
3. Processes the HTML content
4. Indexes the processed content in Elasticsearch

## System Components

```
┌────────────────┐    ┌─────────────────┐    ┌────────────────┐    ┌────────────────┐
│                │    │                 │    │                │    │                │
│   COSMOS API   │───▶│  Async HTML     │───▶│  Content       │───▶│  Elasticsearch │
│   Client       │    │  Downloader     │    │  Processor     │    │  Client        │
│                │    │                 │    │                │    │                │
└────────────────┘    └─────────────────┘    └────────────────┘    └────────────────┘
        │                      │                     │                     │
        └──────────────────────▼─────────────────────▼─────────────────────┘
                            ┌─────────────────┐
                            │                 │
                            │    Pipeline     │
                            │  Orchestrator   │
                            │                 │
                            └─────────────────┘
```

### 1. COSMOS API Client

```python
class CosmosAPIClient:
    """Handles interaction with the COSMOS API."""
    
    def __init__(self, base_url='https://sde-indexing-helper.nasa-impact.net/candidate-urls-api'):
        self.base_url = base_url
        self.session = None
        
    async def stream_collection_urls(self, collection_name, batch_size=100):
        """Stream URLs for a specific collection, yielding batches to conserve memory."""
```

- Streams URL metadata from the COSMOS API in batches using async generators
- Handles pagination internally, yielding each page as a batch
- Memory-efficient design that can handle arbitrarily large collections
- Provides filtering capabilities based on metadata

### 2. Async HTML Downloader

```python
class AsyncHTMLDownloader:
    """Downloads HTML content from URLs concurrently."""
    
    def __init__(self, concurrency_limit=10, timeout=30, retries=3):
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.timeout = timeout
        self.retries = retries
        self.session = None
        
    async def download_batch(self, url_metadata_list):
        """Download HTML from multiple URLs concurrently."""
```

- Uses `aiohttp` for highly concurrent HTTP requests
- Implements connection pooling and semaphore limiting
- Features robust error handling with retries
- Supports both timeouts and rate limiting

### 3. Content Processor

```python
class HTMLProcessor:
    """Processes downloaded HTML content."""
    
    def __init__(self, plugins=None):
        self.plugins = plugins or []
        
    async def process_batch(self, downloaded_batch):
        """Process a batch of HTML content and prepare it for indexing."""
```

- Extracts and processes relevant content from HTML using BeautifulSoup4
- Implements a plugin system for different content processing strategies
- Normalizes content for consistent indexing
- Can handle different document types

### 4. Elasticsearch Client

```python
class ElasticsearchClient:
    """Handles interaction with Elasticsearch."""
    
    def __init__(self, host, index_name, api_key=None):
        self.host = host
        self.index_name = index_name
        self.api_key = api_key
        self.client = None
        
    async def index_batch(self, documents):
        """Index a batch of documents in Elasticsearch."""
```

- Manages connections to your Elasticsearch instance
- Handles bulk indexing operations for efficiency
- Implements document mapping and index management
- Provides error handling for failed indexing operations

### 5. Pipeline Orchestrator

```python
class Pipeline:
    """Orchestrates the entire pipeline."""
    
    def __init__(self, cosmos_api_url, elasticsearch_host, elasticsearch_index, 
                 concurrency=10, batch_size=20):
        self.cosmos_client = CosmosAPIClient(cosmos_api_url)
        self.downloader = AsyncHTMLDownloader(concurrency_limit=concurrency)
        self.processor = HTMLProcessor()
        self.es_client = ElasticsearchClient(elasticsearch_host, elasticsearch_index)
        self.batch_size = batch_size
        
    async def run(self, collection_name):
        """Run the complete pipeline for a collection."""
```

- Coordinates the streaming flow of data between components
- Processes each batch independently as it arrives
- Manages concurrency and batch processing
- Handles error propagation and recovery
- Provides monitoring and logging

## Asynchronous Streaming Strategy

The system uses Python's `asyncio` and generator patterns for efficient streaming:

- **Memory-Efficient Streaming**: Process URL metadata in batches without loading entire dataset
- **Batch Processing**: Handle each batch independently through the pipeline
- **Semaphore Limiting**: Limit concurrent network requests to prevent overloading
- **Connection Pooling**: Reuse HTTP and Elasticsearch connections
- **Parallel Processing**: Use `asyncio.gather()` for parallel processing within each batch

```python
async def run(self, collection_name):
    """Run the complete pipeline for a collection using streaming approach."""
    
    async for url_batch in self.cosmos_client.stream_collection_urls(collection_name, self.batch_size):
        # Download HTML for this batch concurrently
        downloaded_batch = await self.downloader.download_batch(url_batch)
        
        # Process the downloaded content
        processed_batch = await self.processor.process_batch(downloaded_batch)
        
        # Index the processed content
        await self.es_client.index_batch(processed_batch)
```

## Error Handling & Resilience

- **Retries with Backoff**: Implements exponential backoff for transient failures
- **Per-Batch Error Handling**: Errors in one batch don't affect processing of other batches
- **Partial Batch Processing**: Continue processing valid items even if some in a batch fail
- **Dead Letter Queue**: Store failed items for later inspection
- **Detailed Logging**: Comprehensive error information for debugging

## Configuration & Extensibility

```python
# Configuration example
config = {
    'cosmos': {
        'base_url': 'https://sde-indexing-helper.nasa-impact.net/candidate-urls-api',
        'batch_size': 100,
    },
    'downloader': {
        'concurrency': 15,
        'timeout': 30,
        'retries': 3,
    },
    'elasticsearch': {
        'host': 'https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443',
        'index': 'sde_index_custom_scraper',
    }
}
```

- Configuration via YAML, JSON, or environment variables
- Plugin architecture for content processors
- Interface-based design for component replacement

## Usage Example

```python
import asyncio
from cosmos_pipeline import Pipeline

async def main():
    # Initialize the pipeline
    pipeline = Pipeline(
        cosmos_api_url="https://sde-indexing-helper.nasa-impact.net/candidate-urls-api",
        elasticsearch_host="https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
        elasticsearch_index="sde_index_custom_scraper",
        concurrency=10,
        batch_size=20
    )
    
    # Process a specific collection
    collections = ["iau_minor_planet_system"]
    for collection in collections:
        await pipeline.run(collection)

if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

1. Develop and test individual components
2. Implement error handling and monitoring
3. Create configuration management
4. Add metrics collection for performance analysis
5. Implement extensible plugin system for content processors