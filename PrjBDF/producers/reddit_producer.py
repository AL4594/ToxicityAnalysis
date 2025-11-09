import praw
from kafka import KafkaProducer
import json
import time
import sys

# --- Configuration Reddit ---

reddit = praw.Reddit(
    client_id='tktc8C-bBDvIMTUW2YHiHw',
    client_secret='B8LX24vYoT-phgPu1PkJy7pUHuokWg',
    user_agent='ToxicityAppV1'
)
print("Connecté à l'API Reddit")

# --- Configuration Kafka ---
KAFKA_TOPIC = "raw_reddit_comments"
KAFKA_SERVER = "kafka:29092" # Nom du service dans docker-compose

# --- Liste des Subreddits ---
SUBREDDIT_LIST = [
    "news", "worldnews", "politics", "technology", "gaming", "movies", "science",
    "todayilearned", "aww", "funny", "pics", "videos", "AskReddit", "IAmA",
    "dataisbeautiful", "Futurology", "personalfinance", "sports", "Music", "books",
    "history", "LifeProTips", "space", "gadgets", "DIY", "food", "philosophy",
    "Showerthoughts", "explainlikeimfive", "memes", "dankmemes", "wallstreetbets",
    "CryptoCurrency", "stocks", "programming", "Python", "javascript", "MachineLearning",
    "cscareerquestions", "linux", "sysadmin", "devops", "kubernetes", "docker",
    "formula1", "soccer", "nba", "nfl", "leagueoflegends", "Minecraft"
]
# Combinez les subreddits en une seule chaîne pour le streaming
subreddit_string = "+".join(SUBREDDIT_LIST)

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Erreur de connexion Kafka: {e}")
    sys.exit(1)

print(f"Connexion Kafka établie. Streaming des subreddits: {subreddit_string}")

try:
    subreddit = reddit.subreddit(subreddit_string)
    for comment in subreddit.stream.comments(skip_existing=True):
        try:
            data = {
                "id": comment.id,
                "source": "reddit",
                "text_content": comment.body,
                "timestamp": int(comment.created_utc),
                "author": comment.author.name if comment.author else "unknown",
                "subreddit": comment.subreddit.display_name,
                "post_id": comment.link_id,
                "post_title": comment.submission.title
            }
            
            print(f"[REDDIT] -> {data['subreddit']}: {data['text_content'][:50]}...")
            producer.send(KAFKA_TOPIC, value=data)
            
        except Exception as e:
            print(f"Erreur lors du traitement d'un commentaire: {e}")
            
except KeyboardInterrupt:
    print("Arrêt du producteur Reddit.")
except Exception as e:
    print(f"Erreur de stream Reddit: {e}")
finally:
    producer.close()