from flask import Flask, render_template, request, jsonify
from services.es_service import ElasticsearchService

app = Flask(__name__)
es_service = ElasticsearchService()

print("Running Elasticsearch diagnostics...")
es_service.test_connection()
es_service.test_index()


@app.route("/")
def index():
    return render_template(
        "index.html",
        doc_types=[
            "Data",
            "Images",
            "Documentation",
            "Software and Tools",
            "Missions and Instruments",
        ],
    )


@app.route("/search", methods=["POST"])
def search():
    query = request.json.get("query", "")
    doc_types = request.json.get("doc_types", [])
    page = request.json.get("page", 1)

    results, total = es_service.search(
        query_text=query, doc_types=doc_types, page=page, size=25
    )

    return jsonify(
        {
            "results": results,
            "total": total,
            "page": page,
            "pages": (total + 24) // 25,  # Calculate total pages
        }
    )


if __name__ == "__main__":
    app.run(debug=True)
