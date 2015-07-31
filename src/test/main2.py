import itertools as IT
import multiprocessing as mp
import csv
import time
from elasticsearch import Elasticsearch
from httplib2 import Http
from urllib.parse import urlencode
from random import randint
import uuid

csv_header = ""

id_index=1
es= Elasticsearch("http://matteoubuntudev.cloudapp.net:8080",timeout=120)



def worker(chunk):
   bulk_data=[]
   global csv_header
   print(csv_header)
   #csv_header = ["First","last","Id"]
   csv_header = ["year_month","area","week","week_long","tos","atc_1_lvl","atc_1_lvl_desc","atc_2_lvl","atc_2_lvl_desc","atc_3_lvl","atc_3_lvl_desc","atc_4_lvl","atc_4_lvl_desc","atc_5_lvl","atc_5_lvl_desc","pack_code","pack_descr","product_descr","product_manufacturer","pack_product_manufacturer","tot_qty"]
   global id_index
   print("chunk",chunk)
   print("head",csv_header)
   for line in chunk: 
       print(line)
       data_dict={}
       for i in range(len(line)):
          data_dict[csv_header[i]] = line[i]
       op_dict = {
          "index": {
            "_index": "person", 
            "_type": "people", 
            "_id": str(uuid.uuid4())
          }
       }
       bulk_data.append(op_dict)
       bulk_data.append(data_dict)
       id_index=id_index+1

   print(bulk_data[0])
   
   print(es.bulk(index = "person", body = bulk_data, refresh = False))
   '''
   data='{"index":{ "_index":"person", "_type":"people","_id": "zzzz"}}{ "First":"Matteo","last":"zuccon","Id":"zzzz"}}'
   h = Http()
   #data = bulk_data
   h.request('http://matteoubuntudev.cloudapp.net:8080/_bulk', "PUT", data)
   '''
   return len(chunk)


def main():

    global csv_header
    create_request_body={
        "settings": {
            "number_of_shards":1,
            "number_of_replicas":0
        }
    }
    if (es.indices.exists("person")):
        es.indices.delete(index="person")
    else:
        es.indices.create(index="person",body=create_request_body)

    #http://stackoverflow.com/questions/31164731/python-chunking-csv-file-multiproccessing
    start = time.time()
    # num_procs is the number of workers in the pool
    num_procs = mp.cpu_count()
    # chunksize is the number of lines in a chunk
    chunksize = 10
    
    pool = mp.Pool(num_procs)
    largefile = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
    #largefile = "/home/pippo/Documents/Github Repository/Elasticsearch-CSV/dataset/test.csv"
    #largefile = "/home/pippo/Documents/MOCK_DATA.csv"
    #largefile = 'Counseling.csv'
    #largefile = "/home/pippo/Documents/mediumDataset.csv"
    results = []
    with open(largefile, 'r') as f:
        reader = csv.reader(f)
        csv_header=next(reader)
        for chunk in iter(lambda: list(IT.islice(reader, chunksize*num_procs)), []):
            chunk = iter(chunk)
            pieces = list(iter(lambda: list(IT.islice(chunk, chunksize)), []))
            print(csv_header)
            print("pieces",pieces)
            result = pool.map(worker, pieces)
            results.extend(result)
    print(results)
    pool.close()
    pool.join()
    print((time.time()-start)/60)
main()