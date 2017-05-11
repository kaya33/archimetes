from archimedes.api.mongo_base import Mongo

mongo  = Mongo('chaoge')

def fetch_user_profile(user_id, city_name, category):
    data = mongo.read('RecommendationAd',{'_id':""})
    return data['ads']



if __name__ == "__main__":
    fetch_user_profile()