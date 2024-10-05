from elasticsearch import Elasticsearch

index_name = 'products-catalog'
mapping = {
    "settings": {
        "index": {
            "number_of_replicas": 0,
            "number_of_shards": 1,
        }
    },
    "mappings": {
        "properties": {
            "id": {
                "type": "keyword"
            },
            "brand": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                },
            },
            "name": {
                "type": "text"
            },
            "price": {
                "type": "float"
            },
            "price_sign": {
                "type": "keyword"
            },
            "currency": {
                "type": "keyword"
            },
            "image_link": {
                "type": "keyword"
            },
            "description": {
                "type": "text"
            },
            "description_embeddings": {
                "type": "dense_vector",
                "dims": 384
            },
            "rating": {
                "type": "keyword"
            },
            "category": {
                "type": "keyword"
            },
            "product_type": {
                "type": "keyword"
            },
            "tag_list": {
                "type": "keyword"
            }
        }
    }
}


def get_client_es():
    return Elasticsearch(
        hosts=[{'host': 'localhost', 'port': 9200, "scheme": "http"}]
    )


def create_index(index_name, mapping):
    if not get_client_es().indices.exists(index=index_name):
        get_client_es().indices.create(index=index_name, body=mapping)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")


create_index(index_name, mapping)
