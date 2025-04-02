from elasticsearch import Elasticsearch
import os


class ElasticsearchService:
    def __init__(self):
        api_key = os.environ.get("ELASTIC_API_KEY")
        if not api_key:
            raise ValueError("ELASTIC_API_KEY environment variable not set")

        self.es = Elasticsearch(
            "https://my-elasticsearch-project-ce58cf.es.us-east-1.aws.elastic.cloud:443",
            api_key=api_key,
        )
        self.index = "sde_with_vectors"

    def test_connection(self):
        try:
            info = self.es.info()
            print("Connection successful!")
            print(f"Elasticsearch version: {info['version']['number']}")
            return True
        except Exception as e:
            print(f"Connection failed: {str(e)}")
            return False

    def test_index(self):
        try:
            exists = self.es.indices.exists(index=self.index)
            if exists:
                print(f"Index '{self.index}' exists!")
                return True
            else:
                print(f"Index '{self.index}' does not exist.")
                return False
        except Exception as e:
            print(f"Error checking index: {str(e)}")
            return False

    def search(self, query_text, doc_types=None, page=1, size=25):
        # Calculate offset for pagination
        from_val = (page - 1) * size

        # Build a search query that includes both standard text search and vector-based relevance
        must_query = {
            "multi_match": {
                "query": query_text,
                "fields": ["title^2", "text_content"],  # Boost title matches
            }
        }

        # Add document type filter if specified
        query = {"bool": {"must": [must_query]}}

        if doc_types and len(doc_types) > 0:
            query["bool"]["filter"] = [{"terms": {"document_type.keyword": doc_types}}]

        # Execute search
        try:
            response = self.es.search(
                index=self.index,
                body={
                    "query": query,
                    "from": from_val,
                    "size": size,
                    "_source": ["title", "text_content", "url", "document_type"],
                    "highlight": {
                        "fields": {
                            "text_content": {
                                "fragment_size": 150,
                                "number_of_fragments": 1,
                            },
                            "title": {},
                        }
                    },
                },
            )

            # Format results
            results = []
            for hit in response["hits"]["hits"]:
                source = hit["_source"]
                highlight = hit.get("highlight", {})

                # Get title and snippet with highlighting if available
                title = (
                    highlight.get("title", [source.get("title", "Untitled")])[0]
                    if "title" in highlight
                    else source.get("title", "Untitled")
                )

                if "text_content" in highlight and highlight["text_content"]:
                    snippet = highlight["text_content"][0]
                else:
                    text_content = source.get("text_content", "")
                    snippet = text_content[:150] + "..." if text_content else ""

                results.append(
                    {
                        "title": title,
                        "snippet": snippet,
                        "url": source.get("url", "#"),
                        "doc_type": source.get("document_type", "unknown"),
                        "score": hit["_score"],
                    }
                )

            # Get total results count
            total = (
                response["hits"]["total"]["value"] if "total" in response["hits"] else 0
            )

            return results, total

        except Exception as e:
            print(f"Search error: {str(e)}")
            # Return empty results in case of error
            return [], 0
