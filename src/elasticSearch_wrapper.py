from elasticsearch import Elasticsearch

def connect_to_elasticsearch_istance(cluster_ulr):
	print("Connecting to:",cluster_ulr)
	es= Elasticsearch(cluster_ulr)
	print("Connected to:",cluster_ulr)
	return es

def create_index(es_instance,index_name,number_of_shards,number_of_replicas):
	create_request_body={
		"settings":	{
			"number_of_shards":number_of_shards,
			"number_of_replicas":number_of_replicas
		}
	}
	es_instance.indices.create(index=index_name,body=create_request_body)

def check_if_index_exists(es_instance,index_name):
	return es_instance.indices.exists(index_name)

def delete_index(es_instance,index_name):
	es_instance.indices.delete(index=index_name)

def bulk_data(es_instance,index_name,bulk_data):
    return es_instance.bulk(index = index_name, body = bulk_data, refresh = True)