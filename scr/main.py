import sys
import IO_function as IO
import elasticSearch_wrapper as ELS
import constant_name as CST
class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:
       
       csv_lines = IO.get_CSV_rows(CST.DATASET_PATH,";","|")

       csv_header = [item.lower() for item in csv_lines[0]]

       print(csv_header)

       print(CST.DATASET_PATH)
       for line in csv_lines:
       		print(line)

       bulk_data = [] 

for row in csv_file_object:
    data_dict = {}
    for i in range(len(row)):
        data_dict[header[i]] = row[i]
    op_dict = {
        "index": {
        	"_index": INDEX_NAME, 
        	"_type": TYPE_NAME, 
        	"_id": data_dict[ID_FIELD]
        }
    }
    bulk_data.append(op_dict)
    bulk_data.append(data_dict)

       #get elasticsearch instance
       #es_cluster = ELS.connect_to_elasticsearch_istance(CST.ELASTICSEARCH_URL)

       #print(ELS.check_if_index_exists(es_cluster,"bank1"))

       #create the index
       #ELS.create_index(es_cluster,CST.INDEX_NAME,1,0)

       #ELS.delete_index(es_cluster,CST.INDEX_NAME)

       #print(es_cluster.get(index="bank",doc_type="account",id=1))


    except ValueError as err:
        print(err)
        return 2

if __name__ == "__main__":
    sys.exit(main())