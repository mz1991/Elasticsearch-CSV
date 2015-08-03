import itertools as IT
import multiprocessing as mp
import csv
import time
from elasticsearch import Elasticsearch,helpers
import uuid
import re
import datetime
import elasticSearch_wrapper as ELS
import constant_name as CST
import data_function as DTF

#es= Elasticsearch("http://matteoubuntudev.cloudapp.net:8080",timeout=120)
es_instance= ELS.connect_to_elasticsearch_istance(CST.ELASTICSEARCH_URL,CST.TIMEOUT_ELASTICSEARCH)

def bulk_elasticsearch_data(chunk):
   bulk_data=[]
   for line in chunk:
   	    line["week"] = int(line["week"])
   	    line["tot_qty"] = int(line["tot_qty"])
        op_dict = {
	         "_index": CST.INDEX_NAME, 
	         "_type": CST.TYPE_NAME, 
	         "_id": str(uuid.uuid4()),
	         "_source":line
        }
        bulk_data.append(op_dict)
   print("(Success, Fail)",datetime.datetime.now())
   print(helpers.bulk(es_instance,bulk_data,stats_only=True,chunk_size=CST.BULK_CHUNK_SIZE))
   print("Bulk API Called",datetime.datetime.now())


def main():
    if(ELS.check_if_index_exists(es_instance,CST.INDEX_NAME)):
       ELS.delete_index(es_instance,CST.INDEX_NAME)
    ELS.create_index(es_instance,CST.INDEX_NAME,CST.NUMBER_SHARDS,CST.NUMBER_REPLICA)
   
    #http://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing
    start = time.time()
    num_procs = mp.cpu_count()
    #num_procs=1
    chunksize = CST.CHUNK_SIZE
    pool = mp.Pool(num_procs)
    #largefile = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
    #largefile = "/home/pippo/Documents/Github Repository/Elasticsearch-CSV/dataset/test.csv"
    #largefile = "/home/pippo/Documents/MOCK_DATA.csv"
    #largefile = 'Counseling.csv'
    #largefile = "/home/pippo/Documents/mediumDataset.csv"
    with open(CST.DATASET_PATH, 'r') as f:
        reader = csv.DictReader( f,delimiter=";")
        for chunk in iter(lambda: list(IT.islice(reader, chunksize*num_procs)), []):
            chunk = iter(chunk)
            pieces = list(iter(lambda: list(IT.islice(chunk, chunksize)), []))
            result = pool.map_async(bulk_elasticsearch_data,pieces)
            print("Bulk function called",datetime.datetime.now())
    pool.close()
    pool.join()
    print("Done",datetime.datetime.now())
    print("Execution time ",(time.time()-start)/60)
main()