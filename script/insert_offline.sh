source ~/.bashrc
python -u script/insert_offline_to_mongo.py RecommendationUserTagsOffline rsync_data/tag/all_user_tag_final _id tags 
