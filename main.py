
import os
from glob import glob
import datetime
import pprint

from elasticsearch import helpers, Elasticsearch

ROOT_PATH = "./"
mypath = ROOT_PATH + "pelican-blog/"

def list_files(path, fileextension):
    return [y for x in os.walk(path) for y in glob(os.path.join(x[0], '*.{}'.format(fileextension)))]

def import_rst_files():
    all_rst_files = list_files(mypath, "rst")
    docs = []
    for rst_file in all_rst_files:
        client = Elasticsearch('localhost')
        # Readfile content:
        content = read_file_content(rst_file)
        doc = {

            "_index": "blogpost-{}".format(datetime.datetime.now().strftime("%Y-%m-%d")),
            "_type": "blogpost",
            "_id": rst_file,
            "_source": {
                "author": "PelicanblogAuthors",
                "content": content,
            }
        }
        docs.append(doc)
    helpers.bulk(client, docs)

def read_file_content(filename):
    return open(filename, 'r').read().decode('utf-8')

def search_in_elastic(phrase_to_search="blog"):
    from elasticsearch_dsl import Search
    client = Elasticsearch('localhost')
    s = Search(using=client).query('match', content=phrase_to_search)
    response = s.execute()
    return response

def print_found(search_response):
    print pprint.pformat(search_response.hits.hits)

import_rst_files()
found = search_in_elastic()
print_found(found)
