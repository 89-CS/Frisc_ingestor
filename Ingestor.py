# Created by A1C Freeman 89CS MDT 1/11/23
# Assisted in testing by: SrA Reddy

# resource used for removing duplicate documents: https://www.elastic.co/blog/how-to-find-and-remove-duplicate-documents-in-elasticsearch

# This script will convert a json alerts file to a ndjson format
# Then the ndjson will be parsed, indexed and uploaded to elasticsearch
# The final function then gets a hash of all documents using fields provided to look for and remove duplicate entries.

from elasticsearch import Elasticsearch as elasticsearch
from elasticsearch import helpers
import json
import hashlib

dict_of_duplicate_docs = {}

# keys are fields within the indexed document ie {'index':'foo','id':'bar'}
keys_to_include_in_hash = ["alertCategory", "timestamp", "packetNumber"]
es = elasticsearch(
    'kibana IP address -2',
    http_auth=('user', 'pass'),
    # verify certs is disabled because local hosted elasticsearch has no cert
    verify_certs=False
)


class convert_upload_cleanse():
    # has only been tested with [{'key':'value'},{'key':'value'}] format
    def convert_json_to_ndjson():
        with open("alerts.json", "r") as read_file:
            data = json.load(read_file)
        result = [json.dumps(record) for record in data]
        with open("nd-processed-alerts.json", "w+") as obj:
            for i in result:
                obj.write(i+'\n')

    # parses through ndjson, indexes it and uploads.
    def send_json_to_elk(file_name, index_name):
        try:
            with open(file_name) as f:
                for line in f:
                    line = line.replace("\n", "")
                    jdoc = {"data": json.loads(line)}
                    es.index(index=index_name, doc_type='_doc', body=jdoc)
            print('finished upload: ' + index_name)
        except Exception as e:
            print(e)

# Process documents returned by the current search/scroll
    def populate_dict_of_duplicate_docs(hit):
        combined_key = ""
        for mykey in keys_to_include_in_hash:
            combined_key += str(hit['_source']["data"][mykey])
        _id = hit["_id"]
        hashval = hashlib.md5(combined_key.encode('utf-8')).digest()
        dict_of_duplicate_docs.setdefault(hashval, []).append(_id)


if __name__ == "__main__":
    print('starting upload')
    convert_upload_cleanse.convert_json_to_ndjson()
    convert_upload_cleanse.send_json_to_elk(
        "nd-processed-alerts.json", "frisc_log")


# FOR MOVING DEPENDENCIES TO AIR GAP

# ENSURE YOU ARE IN SCRIPT DIRECTORY
# pip download -r requirements.txt -d ./dependencies/

# ENSURE YOU ARE IN SCRIPT DIRECTORY
# pip install --no-index --find-links=./dependencies -r requirements.txt
