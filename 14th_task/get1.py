import os
from dotenv import load_dotenv
import tweepy
import re
from connect import get_db_connection, read_config

# Load variables from .env file
load_dotenv(dotenv_path=r'C:\Users\Manish\Desktop\twitterAPI\config1\config1.env')

# Retrieve Twitter credentials
consumer_key = os.getenv("CONSUMER_KEY")
consumer_secret = os.getenv("CONSUMER_SECRET")
access_token = os.getenv("ACCESS_TOKEN")
access_token_secret = os.getenv("ACCESS_TOKEN_SECRET")
bearer_token = os.getenv("BEARER_TOKEN")  

#Create Tweepy Client for v2
client = tweepy.Client(
    bearer_token=bearer_token,
    consumer_key=consumer_key,
    consumer_secret=consumer_secret,
    access_token=access_token,
    access_token_secret=access_token_secret
)

# Fetch tweets using v2 search
def fetch_tweets_v2(keyword, max_results=10):
    response = client.search_recent_tweets(
        query=keyword,
        max_results=max_results,
        tweet_fields=["text"]
    )
    return [tweet.text for tweet in response.data] if response.data else []

# Clean tweet text
def clean_tweet(text):
    text = re.sub(r"http\S+", "", text)  # Remove URLs
    text = re.sub(r"@\w+", "", text)     # Remove mentions
    text = re.sub(r"#\w+", "", text)     # Remove hashtags
    text = re.sub(r"[^\w\s]", "", text)  # Remove punctuation
    text = re.sub(r"\d+", "", text)      # Remove numbers
    return text.lower().strip()

# Insert tweets into SQL Server
def insert_tweets_to_sql(tweets, config):
    conn = get_db_connection(config)
    cursor = conn.cursor()

    # Create table if it doesn't exist
    cursor.execute("""
        IF NOT EXISTS (
            SELECT * FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_NAME = 'tweets1'
        )
        CREATE TABLE tweets1 (
            id INT IDENTITY(1,1) PRIMARY KEY,
            tweet_text NVARCHAR(MAX)
        )
    """)
    conn.commit()

    for tweet in tweets:
        cleaned = clean_tweet(tweet)
        print("Cleaned:", cleaned)
        cursor.execute("INSERT INTO tweets1 (tweet_text) VALUES (?)", cleaned)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{len(tweets)} tweets inserted into SQL Server table 'tweets1'")

#  Entry point
if __name__ == "__main__":
    config = read_config()
    raw_tweets = fetch_tweets_v2("Python", max_results=10)
    insert_tweets_to_sql(raw_tweets, config)
