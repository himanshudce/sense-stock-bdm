# import libraries
# from itertools import count
import tweepy
import json
import pandas as pd
# import time
import os



def save_json(json_location,tw_dict):
    with open(json_location, 'w') as f:
        json.dump(tw_dict, f, indent=2)
    print("data saved to disk at location",json_location)



def load_json(json_location):
    with open(json_location) as json_file:
        data = json.load(json_file)
    print("data loaded from the disk \n")
    return data




# Keys
api_key = 'YnexwDPzFJqX61H9BBOQvEUBx'
api_key_secret = 'lUE47pTi98oadCkfoiednY2B9P1QwH2YsZamwJ7jMiywNfl6tu'
access_token = '1181621632768462848-Q3Q37PBA4ohatLGSJwX0BpMs8cQjII'
access_token_secret = 'TGYmhkg1u7YMgLUPat0CpwqYKVLgpJLs0YKsiCiw8gcRq'



# authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

companies = pd.read_csv('top_25NSE.csv')

companies_list = list(companies['Company Name'].values)

# important business kewords
keywords_stock = ['finance','india','business','nse','sensex','bank','investment','stock','money','funds']

# major searches we need
query_list = companies_list[:25] + keywords_stock


# making dict to save data
hour_tweet_dict = dict()
date = pd.Timestamp.now('Asia/Kolkata')
hour = date.hour


# extracting tweet data
print('extracting tweet data')
hour_ls = []
for query in query_list:
    try:
        tweets = tweepy.Cursor(api.search_tweets, q=query,result_type='popular',tweet_mode = "extended",lang="en").items(100)
    except:
        break
    
    for tweet in tweets:
        json_d = tweet._json
        date = pd.Timestamp.now('Asia/Kolkata')
        json_d['timestamp'] = date.timestamp()
        json_d['hour'] = date.hour
        hour_ls.append(json_d)

print('extracted tweet data for date {}'.format(date))


# hour_ls
# save in dict with value hour
hour_tweet_dict[str(date.hour)] = hour_ls



# saving paths
base_location = 'data_sources/tweet_data'
json_path = str(date.date()).replace("-","_") +'.json'
json_save_location = os.path.join(base_location,json_path)


try:
    os.makedirs(base_location)
except:
    pass


try:
    data = load_json(json_save_location)
    data[str(hour)] = hour_tweet_dict
    save_json(json_save_location,data)
except:
    save_json(json_save_location,hour_tweet_dict)



