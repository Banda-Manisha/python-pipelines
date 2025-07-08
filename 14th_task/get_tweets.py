import os
import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=r"C:\Users\Manish\Desktop\twitterAPI\config.env") 
BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

query = '("BeanBrew" OR #WakeUpWithBeanBrew) lang:en -is:retweet'
url = "https://api.twitter.com/2/tweets/search/recent"

headers = {"Authorization":f"Bearer {BEARER_TOKEN}"}
params = {
    "query": '("BeanBrew" OR #WakeUpWithBeanBrew) lang:en -is:retweet',
    "tweet.fields": "created_at,text,lang",
    "max_results": 10
}


response = requests.get(url,headers=headers,params=params)

if response.status_code == 200:
    tweets = response.json()
    for tweet in tweets.get("data",[]):
        print(f"{tweet['text']}\n")
else:
    print(f"ERROR:{response.status_code} - {response.text}")
