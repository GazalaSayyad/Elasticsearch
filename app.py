from elasticsearch import helpers,Elasticsearch
import json


__data__ = {
    "Name": "Gazala",
    "gender": "Female",
    "age": "24",
    "body_fat": "15%",
    "interest": ["NLP", "ELK"]
    }

def connect_elasticsearch():
    es = None
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    if es.ping():
        print('Elastic search Connected...')
    else:
        print('ohh...it could not connect!')
    return es



def create_index(index):
    es =connect_elasticsearch()
    es.indices.create(index=index, ignore=400)

def single_data_insert(index, data):
    es =connect_elasticsearch()
    # index and doc_type you can customize by yourself
    res = es.index(index=index, doc_type='intro', id=5, body=__data__)
    # index will return insert info: like as created is True or False
    print(res)
    return res


#insert_bulk_data
def insert_data_by_bulk():
    es =connect_elasticsearch()
    op_list = []
    with open("shakespeare_6.0.json") as json_file:
        for record in json_file:
            op_list.append({
                '_op_type': 'index',
                '_index': 'shakes',
                '_type': 'play',
                '_source': record
                })
    helpers.bulk(es,actions=op_list)

def delete_index(index):
    es =connect_elasticsearch()
    res = es.indices.delete(index=index, ignore=[400, 404])
    print("Deleted index.....")
    return res

if __name__ == "__main__":
    connect_elasticsearch()
    create_index("test")
    single_data_insert(index='test',data=__data__)
    insert_data_by_bulk()
    delete_index(index='shakes')
  