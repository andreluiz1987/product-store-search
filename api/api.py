from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

promote_products_free_gluten = ["HryOV5IBcEU5Aj1dkOQa", "j7yOV5IBcEU5Aj1dkORG", "WbyOV5IBcEU5Aj1dkOQa"]


def get_client_es():
    return Elasticsearch(
        hosts=[{'host': 'localhost', 'port': 9200, "scheme": "http"}]
    )


def build_query(term=None, categories=None, product_types=None, brands=None):
    must_query = [{"match_all": {}}] if not term else [{
        "multi_match": {
            "query": term,
            "fields": ["name", "category", "description"]
        }
    }]

    filters = []
    if categories:
        filters.append({"terms": {"category": categories}})
    if product_types:
        filters.append({"terms": {"product_type": product_types}})
    if brands:
        filters.append({"terms": {"brand.keyword": brands}})

    return {
        "_source": ["id", "brand", "name", "price", "currency", "image_link", "category", "tag_list"],
        "query": {
            "bool": {
                "must": must_query,
                "filter": filters
            }
        }
    }


def search_products(term, categories=None, product_types=None, brands=None, promote_products=[]):
    organic_query = build_query(term, categories, product_types, brands)

    if promote_products:
        query = {
            "query": {
                "pinned": {
                    "ids": promote_products,
                    "organic": organic_query['query']
                }
            },
            "_source": organic_query['_source']
        }
    else:
        query = organic_query

    response = get_client_es().search(index="products-catalog", body=query, size=20)

    return [
        {
            "id": hit['_source']['id'],
            "brand": hit['_source']['brand'],
            "name": hit['_source']['name'],
            "price": hit['_source']['price'],
            "currency": hit['_source']['currency'] if hit['_source']['currency'] else "USD",
            "image_link": hit['_source']['image_link'],
            "category": hit['_source']['category'],
            "tags": hit['_source'].get('tag_list', [])
        }
        for hit in response['hits']['hits']
    ]


def get_facets_data(term, categories=None, product_types=None, brands=None):
    query = build_query(term, categories, product_types, brands)
    query["aggs"] = {
        "product_types": {"terms": {"field": "product_type"}},
        "categories": {"terms": {"field": "category"}},
        "brands": {"terms": {"field": "brand.keyword"}}
    }
    response = get_client_es().search(index="products-catalog", body=query, size=0)

    return {
        "product_types": [
            {"product_type": bucket['key'], "count": bucket['doc_count']}
            for bucket in response['aggregations']['product_types']['buckets']
        ],
        "categories": [
            {"category": bucket['key'], "count": bucket['doc_count']}
            for bucket in response['aggregations']['categories']['buckets']
        ],
        "brands": [
            {"brand": bucket['key'], "count": bucket['doc_count']}
            for bucket in response['aggregations']['brands']['buckets']
        ]
    }


@app.route('/api/products/search', methods=['GET'])
def search():
    query = request.args.get('query')
    categories = request.args.getlist('selectedCategories[]')
    product_types = request.args.getlist('selectedProductTypes[]')
    brands = request.args.getlist('selectedbrands[]')
    results = search_products(query, categories=categories, product_types=product_types,
                              brands=brands,
                              promote_products=[])
    return jsonify(results)


@app.route('/api/products/facets', methods=['GET'])
def facets():
    query = request.args.get('query')
    categories = request.args.getlist('selectedCategories[]')
    product_types = request.args.getlist('selectedProductTypes[]')
    brands = request.args.getlist('selectedbrands[]')
    results = get_facets_data(query, categories=categories, product_types=product_types,
                              brands=brands)
    return jsonify(results)


if __name__ == '__main__':
    app.run(debug=True)
