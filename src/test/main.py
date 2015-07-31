import multiprocessing as mp
import itertools
import time
import csv

def worker(es,chunk):
    # `chunk` will be a list of CSV rows all with the same name column
    # replace this with your real computation
    
    #print(chunk)
     


    return len(chunk)  

def keyfunc(row):
    # `row` is one row of the CSV file.
    # replace this with the name column.
    return row[0]

def main():
	

    pool = mp.Pool()
    start = time.time()
    #largefile = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
    largefile = "/home/pippo/Documents/Github Repository/Elasticsearch-CSV/dataset/test.csv"
    num_chunks = 100
    #results = []
    with open(largefile) as f:
        reader = csv.reader(f)
        print(reader.first())
        chunks = itertools.groupby(reader, keyfunc)
        while True:
            # make a list of num_chunks chunks
            groups = [list(chunk) for key, chunk in
                      itertools.islice(chunks, num_chunks)]
            if groups:
                #a=list(itertools.chain(*list(itertools.chain(*groups))))
                #result = pool.map(worker, [a])
                #result = pool.map_async(worker, [a])
                #result = pool.map_async(worker, groups)
                result = pool.map(es,worker, groups)
                #results.extend(result)
            else:
                break
    pool.close()
    pool.join()
    #print(results)
    print((time.time()-start)/60)

if __name__ == '__main__':
    main()