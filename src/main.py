import itertools as IT
import multiprocessing as mp
import csv
import time
from elasticsearch import Elasticsearch,helpers
from httplib2 import Http
from urllib.parse import urlencode
from random import randint
import uuid
import re
import datetime
import constant_name as CST

es= Elasticsearch(CST.ELASTICSEARCH_URL,timeout=300,http_auth=(CST.ELASTICSEARCH_USER,CST.ELASTICSEARCH_PASS))

def bulk_data(chunk):
   print("into worker",datetime.datetime.now())
   bulk_data=[]
   for line in chunk:
        line["week"] = int(line["week"])
        line["tot_qty"] = int(line["tot_qty"])
        op_dict = {
	         "_index": CST.INDEX_NAME, 
	         "_type": CST.TYPE_NAME, 
	         "_source":line
        }
        bulk_data.append(op_dict)

   print("(Success, Fail)",datetime.datetime.now())
   bulk_response=helpers.bulk(es,bulk_data,stats_only=False,chunk_size=CST.BULK_CHUNK_SIZE)
   print(bulk_response[0])
   print("API Called",datetime.datetime.now())

def main():


    create_request_body={
        "settings": {
            "number_of_shards":CST.NUMBER_SHARDS,
            "number_of_replicas":CST.NUMBER_REPLICA
        }
    }

    mapping_request_body=CST.TYPE_MAPPING

    index_name= CST.INDEX_NAME
    if (es.indices.exists(index_name)):
        es.indices.delete(index=index_name)
    else:
        es.indices.create(index=index_name,body=create_request_body)
        es.indices.put_mapping(doc_type=CST.TYPE_NAME,index=index_name,body=mapping_request_body)
   
    #http://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing
    start = time.time()
    num_procs = mp.cpu_count()
    
    pool = mp.Pool(num_procs)
    largefile = CST.DATASET_PATH
    with open(largefile, 'r') as f:
        reader = csv.DictReader( f,delimiter=";")
        for chunk in iter(lambda: list(IT.islice(reader, CST.CHUNK_SIZE*num_procs)), []):
            chunk = iter(chunk)
            pieces = list(iter(lambda: list(IT.islice(chunk, CST.CHUNK_SIZE)), []))
            result = pool.map_async(bulk_data,pieces)
            print("Worker called")
    pool.close()
    pool.join()
    print("Done")
    print((time.time()-start)/60)
main()
