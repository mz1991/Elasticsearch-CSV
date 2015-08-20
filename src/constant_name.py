
### DATASET ###

DATASET_PATH = "/home/pippo/py-load-data/dataset/testdataset/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"
#DATASET_PATH = "/home/pippo/Documents/CIL_EuroImpact_apr_jul_2015_All_ATC_prd_pha_all_10jul2015.csv"

CHUNK_SIZE = 4000


### ELASTICSEARCH ###

#ELASTICSEARCH_URL = "http://matteoubuntudev.cloudapp.net:8080"
ELASTICSEARCH_URL = "http://localhost:9200"

ELASTICSEARCH_USER ="es_admin"
ELASTICSEARCH_PASS ="esadmin"

BULK_CHUNK_SIZE = 3000

INDEX_NAME = "agosto"
NUMBER_SHARDS = 1
NUMBER_REPLICA = 0
TYPE_NAME = "pharmacy"

TYPE_MAPPING = {
    TYPE_NAME: {
        "properties": {
            "area": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_1_lvl": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_1_lvl_desc": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_2_lvl": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_2_lvl_desc": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_3_lvl": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_3_lvl_desc": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_4_lvl": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_4_lvl_desc": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_5_lvl": {
                "type": "string",
                "index": "not_analyzed"
            },
            "atc_5_lvl_desc": {
                "type": "string",
                "index": "not_analyzed"
            },
            "pack_code": {
                "type": "string",
                "index": "not_analyzed"
            },
            "pack_descr": {
                "type": "string",
                "index": "not_analyzed"
            },
            "pack_product_manufacturer": {
                "type": "string",
                "index": "not_analyzed"
            },
            "product_descr": {
                "type": "string",
                "index": "not_analyzed"
            },
            "product_manufacturer": {
                "type": "string",
                "index": "not_analyzed"
            },
            "tos": {
                "type": "string",
                "index": "not_analyzed"
            },
            "tot_qty": {
                "type": "long"
            },
            "week": {
                "type": "long"
            },
            "week_long": {
                "type": "string",
                "index": "not_analyzed"
            },
            "year_month": {
                "type": "date",
                "format":"YYYY-MM"
            }
        }
    }
}



