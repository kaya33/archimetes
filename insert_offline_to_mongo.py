import json
import time
import bson
import sys
import datetime
import ast
import pymongo
from src.api.mongo import Mongo
if len(sys.argv) < 4:
    print('usage: collect_name file_name key val')
    exit()

collect_name = sys.argv[1]
file_name = sys.argv[2]
key = sys.argv[3]
val = sys.argv[4]
with open(file_name, 'r')as f:

    total_list = []
    mongo_driver = Mongo('chaoge', 0)
    mongo_driver.connect()
    tmp_dict = {}
    for index, row in enumerate(f):
        if len(row) < 3:
            continue
        a, b = row.split('\t')
        tmp_dict[key] = a
        tmp_dict['update_time'] = datetime.datetime.utcnow()
        try:
            tmp_dict[val] = json.loads(b)
            #tmp_dict[val] = ast.literal_eval(b)
        except:
            continue
        total_list.append(dict(tmp_dict))
        if index % 1000 == 0:
            print('line:{0}, time:{1}'.format(index, time.time()))
        if len(total_list) == 100:
            try:
                mongo_driver.insert(collect_name, total_list)
            except (bson.errors.InvalidDocument, pymongo.errors.BulkWriteError):
                total_list = []
            #    print(total_list)
                continue
            total_list = []

if len(total_list) > 0:
    mongo_driver.insert(collect_name, total_list)
