
### DATASET ####

DATASET_PATH = "/home/pippo/py-load-data/dataset/testdataset/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
#DATASET_PATH = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"

CHUNK_SIZE = 4000


### ELASTICSEARCH ###

#ELASTICSEARCH_URL = "http://matteoubuntudev.cloudapp.net:8080"
ELASTICSEARCH_URL = "http://localhost:9200"

INDEX_NAME = "testindice"

TYPE_NAME = "pharmacy"

ID_FIELD =""

TIMEOUT_ELASTICSEARCH=300

BULK_CHUNK_SIZE = 2000

NUMBER_SHARDS = 1

NUMBER_REPLICA = 0