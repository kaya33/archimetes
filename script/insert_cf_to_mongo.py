import json
import time
import bson
import sys
import datetime
import ast
import pymongo
from api.mongo_base import Mongo

if len(sys.argv) < 4:
    print('usage: collect_name file_name key val')
    exit()

collect_name = sys.argv[1]
file_name = sys.argv[2]
key = sys.argv[3]
val = sys.argv[4]
with open(file_name, 'r')as f:

    mongo_driver = Mongo('chaoge', 0)
    mongo_driver.connect()
    tmp_dict = {}
    for index, row in enumerate(f):
        if len(row) < 3:
            continue
        a, b = row.split('\t')
        b = b.replace("'", "\"").replace("u", "")
        tmp_dict[key] = a + '&itemCF&1'
        tmp_dict['update_time'] = datetime.datetime.utcnow()
        try:
            tmp_dict[val] = json.loads(b)
            #tmp_dict[val] = ast.literal_eval(b)
        except:
            print(b)
            continue
        if index % 20000 == 0:
            print('line:{0}, time:{1}'.format(index, time.time()))

        try:
            mongo_driver.update(collect_name, key, dict(tmp_dict))

        except (bson.errors.InvalidDocument, pymongo.errors.BulkWriteError):
            continue

