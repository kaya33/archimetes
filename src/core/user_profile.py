from src.api.mongo import Mongo

mongo  = Mongo('chaoge')

def fetch_user_profile():
    data = mongo.read('RecommendationAd',{'_id':'999800273&itemCF&1'})
    return data['ads']

if __name__ == "__main__":
    fetch_user_profile()