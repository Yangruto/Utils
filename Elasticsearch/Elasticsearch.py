import json
import os
import sys
import csv
import pandas as pd
import elasticsearch
import argparse
from argparse import RawTextHelpFormatter
from elasticsearch import helpers
from elasticsearch import Elasticsearch

parser = argparse.ArgumentParser(description='Get articles from Elasticsearch')
parser.add_argument("-category", help="Article category")
args = parser.parse_args()

# get data by id
# es.get(index='webpages', doc_type='_doc', id='6f60140cf0458d623c5322e8c2c3d9f7bd239df0')


# connect to elk
es = Elasticsearch("hostname")

# set query
query = {
	"_source":['title', 'hostname', 'url', 'category', 'content_clean', 'page_id', 'created_time'],
	"query": {
	"match": {
	"category" : args.category
		}
	}	
}

# create generator
ge = helpers.scan(client=es, query=query, index='webpages', scroll="10m", size=1000, request_timeout=3000)

# get data
data = []
for i, j in enumerate(ge):
	data.append(j['_source'])
	print(f'No.{i} {j["_source"]["category"]} {i["_source"]["created_time"]}')
data = pd.DataFrame(data)
data.to_csv(f'./elk_{args.category}.csv', index=False, quoting=csv.QUOTE_ALL)