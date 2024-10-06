import json

from elasticsearch import Elasticsearch, helpers
from sentence_transformers import SentenceTransformer


def get_client_es():
    return Elasticsearch(
        hosts=[{'host': 'localhost', 'port': 9200, "scheme": "http"}]
    )


def get_text_vector(sentences):
    model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
    embeddings = model.encode(sentences)
    return embeddings


def read_json_file(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def chunk_data(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def generate_bulk_actions(index_name, data_batch):
    for item in data_batch:
        document_id = item['id']
        item['description_embeddings'] = get_text_vector(item['description'])
        yield {
            "_index": index_name,
            "_id": document_id,
            "_source": item
        }


def index_data_in_batches(file_path, index_name, batch_size=100):
    data = read_json_file(file_path)

    for batch in chunk_data(data, batch_size):
        actions = generate_bulk_actions(index_name, batch)
        success, failed = helpers.bulk(get_client_es(), actions)
        print(f"Batch indexed: {success} successful, {failed} failed")


if __name__ == '__main__':
    # print(get_client_es().info())
    index_data_in_batches("../files/dataset/products.json", "products-catalog", batch_size=100)
