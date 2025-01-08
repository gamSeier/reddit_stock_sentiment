import praw
from kafka import KafkaProducer
import json

# Reddit api keys
reddit = praw.Reddit(client_id=CLIENT_ID,
                     client_secret=CLIENT_SECRET,
                     user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36')

# Kafka producer configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Stream live data from subreddit
def stream_reddit_data(subreddit_list):
    reddit_threads = '+'.join(subreddit_list)
    subreddit = reddit.subreddit(reddit_threads)

    for comment in subreddit.stream.comments(skip_existing=True):
        try:
            post = comment.submission
            data = {
                'subreddit': comment.subreddit.display_name,
                'body': comment.body,
                'comment_author': comment.author.name if comment.author else '[deleted]',
                'created_utc': int(comment.created_utc),
                'post_title': post.title,
                'score': post.score
            }
            producer.send('reddit', value=data)
            print(f"Sent post from {data['subreddit']}: {data['body']} {data['created_utc']}")
        except Exception as e:
            print(f"Error processing comment: {e}")

subreddits = ['wallstreetbets', 'finance', 'StockMarket', 'investing', 'Daytrading', 'stocks', 'personalfinance',
              'ProfessorFinance', 'ValueInvesting']

# stream the live comments
stream_reddit_data(subreddits)
