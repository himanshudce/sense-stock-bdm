import pymongo
import pandas as pd
import os
import json
# myclient = pymongo.MongoClient("mongodb://10.4.41.42:27017/")
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
tweetdb = mongo_client["tweetdb"]



# loading data from json
def load_json(json_location):
    with open(json_location) as json_file:
        data = json.load(json_file)
    print("data loaded from the disk \n")
    return data



# date of the day
date = pd.Timestamp.now('Asia/Kolkata')


# working on tweet database
col = 'date'+'_'+str(date.date()).replace("-","_")

# collection in the database
tw_date_col = tweetdb[col]




# json saved path
base_location = 'data_sources/tweet_data'
json_path = str(date.date()).replace("-","_") +'.json'
json_save_location = os.path.join(base_location,json_path)

data = load_json(json_save_location)

for key,val in data.items():
    ins = tw_date_col.insert_many(val)

print('object ids loaded are - ',ins.inserted_ids)
print("================================================================================================")




# =========================
# basic mongo queries
# to find
# temp = tweetdb.tw_date_col.find()
# for x in tw_date_col.find():
#   print(x)


# to drop collection
# tw_date_col.drop()