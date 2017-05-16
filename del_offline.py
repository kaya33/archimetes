import json
import bson
import sys
from src.api.mongo import Mongo

if len(sys.argv) < 2:
    name = 'RecommendationUserTagsOffline'
else:
    name = sys.argv[1]
mongo_driver = Mongo('chaoge', 0)
mongo_driver.connect()
mongo_driver.delete(name)
