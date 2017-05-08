from src.api.mongo import Mongo

mongo  = Mongo('chaoge')

def fetch_user_profile(user_id, city_name, category):
    data = mongo.read('RecommendationAd',{'_id':""})
    return data['ads']

def fetch_batch_itemrec(item_id, rec_name = "itemCF", id_type = 1):
    data = mongo.read("RecommendationAd",{"_id":item_id+rec_name+id_type})

if __name__ == "__main__":
    fetch_batch_itemrec()