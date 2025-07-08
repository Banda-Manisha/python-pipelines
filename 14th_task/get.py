#get.py
import tweepy
import pandas as pd
import os
from dotenv import load_dotenv
from connect import get_db_connection,read_config
import re

load_dotenv(dotenv_path=r'C:\Users\Manish\Desktop\twitterAPI\config.env')
BEARER_TOKEN = os.getenv("BEARER_TOKEN")

if not BEARER_TOKEN:
    raise ValueError("Bearer token not found.Please check your.env file")


client = tweepy.Client(bearer_token=BEARER_TOKEN)

def fetch_tweets(query="iphone15 -is:retweet lang:en", max_results=100):
    """Fetch tweets using Tweepy client"""
    response = client.search_recent_tweets(
        query=query,
        max_results=max_results,
        tweet_fields=["created_at", "text", "author_id"]
    )
    return response.data if response.data else []

def clean_tweet(text):
    """Clean tweet text: remove URLs, mentions, hashtags, emojis, and extra spaces"""
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#\w+", "", text)
    text = re.sub(r"[^\x00-\x7F]+", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

def filter_tweet(text):
    """Return True if the tweet passes the keyword filter"""
    keywords = ['buy', 'review', 'awesome']
    return any(word in text.lower() for word in keywords)

def save_to_sql(tweets):
    """Save cleaned and filtered tweets to SQL Server"""
    config = read_config()
    conn = get_db_connection(config)
    cursor = conn.cursor()

    inserted_count = 0

    for tweet in tweets:
        cleaned = clean_tweet(tweet.text)

        if filter_tweet(cleaned):
            try:
                cursor.execute("""
                    INSERT INTO Tweets (tweet_id, text, created_at, author_id)
                    VALUES (?, ?, ?, ?)
                """, tweet.id, cleaned, tweet.created_at, tweet.author_id)
                inserted_count += 1
            except Exception as e:
                print(f"Could not insert tweet {tweet.id}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"{inserted_count} tweets inserted into SQL Server.")

# ====================== MAIN EXECUTION ======================

def main():
    tweets = fetch_tweets()
    if tweets:
        save_to_sql(tweets)
    else:
        print("No tweets found.")

if __name__ == "__main__":
    main()
