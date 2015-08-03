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

csv_header = ""

id_index=1
#es= Elasticsearch("http://matteoubuntudev.cloudapp.net:8080",timeout=120)
es= Elasticsearch("http://localhost:9200",timeout=300)


def worker(chunk):
   print("into worker",datetime.datetime.now())
   #csv_header = ["year_month","area","week","week_long","tos","atc_1_lvl","atc_1_lvl_desc","atc_2_lvl","atc_2_lvl_desc","atc_3_lvl","atc_3_lvl_desc","atc_4_lvl","atc_4_lvl_desc","atc_5_lvl","atc_5_lvl_desc","pack_code","pack_descr","product_descr","product_manufacturer","pack_product_manufacturer","tot_qty"]
   '''
   for line in chunk: 
       #print(line)
       data_dict={}
       for i in range(len(line)):
          if((csv_header[i] =="week") or (csv_header[i]=="tot_qty")):
             #print("zzz",int(line[i]))
             data_dict[csv_header[i]] = int(str(line[i]))
             #data_dict[csv_header[i]] = re.escape(line[i])
          else:
             data_dict[csv_header[i]] = re.escape(line[i])
          op_dict = {
             "_index": "pharmadev", 
             "_type": "pharma", 
             "_id": str(uuid.uuid4()),
             "_source":data_dict
          }
      
       bulk_data.append(op_dict)
       #bulk_data.append(data_dict)
       #id_index=id_index+1
   '''
   
   bulk_data=[]
   for line in chunk:
        op_dict = {
	         "_index": "pharmadev", 
	         "_type": "pharma", 
	         "_id": str(uuid.uuid4()),
	         "_source":line
        }
        bulk_data.append(op_dict)

   print("(Success, Fail)",datetime.datetime.now())
   print(helpers.bulk(es,bulk_data,stats_only=True,chunk_size=2000))
   print("API Called",datetime.datetime.now())
   
   return len(chunk)


def main():

    global csv_header
    create_request_body={
        "settings": {
            "number_of_shards":1,
            "number_of_replicas":0
        }
    }
    index_name= "pharmadev"
    if (es.indices.exists(index_name)):
        es.indices.delete(index=index_name)
    else:
        es.indices.create(index=index_name,body=create_request_body)
   
    #http://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing
    start = time.time()
    # num_procs is the number of workers in the pool
    num_procs = mp.cpu_count()
    #num_procs=1
    # chunksize is the number of lines in a chunk
    chunksize = 2000
    
    pool = mp.Pool(num_procs)
    #largefile = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
    #largefile = "/home/pippo/Documents/Github Repository/Elasticsearch-CSV/dataset/test.csv"
    #largefile = "/home/pippo/Documents/MOCK_DATA.csv"
    #largefile = 'Counseling.csv'
    #largefile = "/home/pippo/Documents/mediumDataset.csv"
    largefile = "/home/pippo/py-load-data/dataset/testdataset/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
    #largefile="/home/pippo/py-load-data/dataset/testdataset/mediumDataset.csv"
    results = []
    with open(largefile, 'r') as f:
        #fieldnames =("year_month","area","week","week_long","tos","atc_1_lvl","atc_1_lvl_desc","atc_2_lvl","atc_2_lvl_desc","atc_3_lvl","atc_3_lvl_desc","atc_4_lvl","atc_4_lvl_desc","atc_5_lvl","atc_5_lvl_desc","pack_code","pack_descr","product_descr","product_manufacturer","pack_product_manufacturer","tot_qty")
        reader = csv.DictReader( f,delimiter=";")
        csv_header=next(reader)
        for chunk in iter(lambda: list(IT.islice(reader, chunksize*num_procs)), []):
            chunk = iter(chunk)
            pieces = list(iter(lambda: list(IT.islice(chunk, chunksize)), []))
            #print(csv_header)
            #print("pieces",pieces)
            bulk_data=[]
            result = pool.map_async(worker,pieces)
            print("Worker called")
    print(results)
    pool.close()
    pool.join()
    print("Done")
    print((time.time()-start)/60)
main()