from pymongo import MongoClient
import pprint

client = MongoClient('192.168.1.40', 27017)





def fetchRecData():
    db = client['chaoge']
    coll = db['RecommenderationAd']
    many_docs = coll.find({"_id": "1000055924&itemCF&1"})
    for doc in many_docs:
        pprint.pprint(many_docs)

if __name__ == "__main__":
    fetchRecData()