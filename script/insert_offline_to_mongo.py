from api.mongo import Mongo
import json

with open('test', 'r')as f:
    data = f.read().split('\n')

total_list = []
mongo_driver = Mongo('chaoge')

for row in data:
    tmp_dict = {}
    a, b = row.split('\t')
    tmp_dict['user_id'] = a
    tmp_dict['tags'] = json.loads(b)
    total_list.append(tmp_dict)
    if len(total_list) == 100:
        mongo_driver.insert('RecommendationUserTagsOffline', total_list)
        total_list = []

if len(total_list) > 0:
    mongo_driver.insert('RecommendationUserTagsOffline', total_list)