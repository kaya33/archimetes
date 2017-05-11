import json
from pprint import pprint
from ..api import mongo_base

from archimedes.utils.parse_json import list_all_dict,get_all_keywd
from archimedes.data.base_data_source import fetchKwData


def fetch_batch_itemrec(ad_id, rec_name = "itemCF", id_type = "1"):
    data = mongo.read("RecommendationAd",{"_id":ad_id+"&"+rec_name+"&"+id_type}).next()
    item_list = data['ads']
    return item_list


def fetch_batch_userrec(user_id,size=3):
    data = mongo.read('RecommendationUserTagsOffline', {'user_id':user_id}).next()
    tags = get_all_keywd(data['tags'])[10]
    kwdata = {"num": size,"city": "shanghai","category": "wupinhuishou","tag": "_".join(tags),"days": 400}
    fetchKwData(kwdata)
