import sys
import IO_function as IO
import elasticSearch_wrapper as ELS
import constant_name as CST
import data_function as DTF

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
       
       csv_lines = IO.get_CSV_rows(CST.DATASET_PATH,";","|")
       csv_header = [item.lower() for item in csv_lines[0]]

       bulk_data = DTF.create_bulk_data(csv_lines,csv_header,CST.INDEX_NAME,CST.TYPE_NAME)
       #print(bulk_data)


       #get elasticsearch instance
       es_cluster = ELS.connect_to_elasticsearch_istance(CST.ELASTICSEARCH_URL)
       
       if(ELS.check_if_index_exists(es_cluster,CST.INDEX_NAME)):
          ELS.delete_index(es_cluster,CST.INDEX_NAME)

       ELS.create_index(es_cluster,CST.INDEX_NAME,1,0)
       print("bulk indexing...")

       print(ELS.bulk_data(es_cluster,CST.INDEX_NAME,bulk_data))


    except ValueError as err:
        print(err)
        return 2

if __name__ == "__main__":
    sys.exit(main())