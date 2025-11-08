from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import time
import sys
import random

# --- Configuration YouTube (Utilisez votre clé !) ---
# !! ATTENTION : NE PAS EXPOSER CETTE CLÉ PUBLIQUEMENT !!
API_KEY = "AIzaSyDy4FsuW09beKfCqv8oPtAeCdRCDsOPibE"
SEARCH_QUERIES = ["débat politique", "actualités", "gaming review", "tech explained"]
MAX_VIDEOS_PER_QUERY = 2 # Limite pour ne pas épuiser le quota
MAX_COMMENTS_PER_VIDEO = 5 # Limite pour ne pas épuiser le quota

# --- Configuration Kafka ---
KAFKA_TOPIC = "raw_youtube_comments"
KAFKA_SERVER = "kafka:29092"

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    youtube = build('youtube', 'v3', developerKey=API_KEY)
    print("Connecté à Kafka et à l'API YouTube")
except Exception as e:
    print(f"Erreur de connexion: {e}")
    sys.exit(1)

seen_comment_ids = set()

def get_video_comments(video_id, video_title):
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=MAX_COMMENTS_PER_VIDEO,
            order="time" # Pour avoir les plus récents
        )
        response = request.execute()

        for item in response.get("items", []):
            comment = item["snippet"]["topLevelComment"]["snippet"]
            comment_id = item["snippet"]["topLevelComment"]["id"]
            
            if comment_id not in seen_comment_ids:
                data = {
                    "id": comment_id,
                    "source": "youtube",
                    "text_content": comment["textDisplay"],
                    "timestamp": int(time.mktime(time.strptime(comment["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"))),
                    "author": comment.get("authorDisplayName", "unknown"),
                    "video_id": video_id,
                    "video_title": video_title
                }
                
                print(f"[YOUTUBE] -> {data['video_title'][:20]}...: {data['text_content'][:50]}...")
                producer.send(KAFKA_TOPIC, value=data)
                seen_comment_ids.add(comment_id)
                
    except Exception as e:
        if 'commenting is disabled' in str(e).lower():
            print(f"Commentaires désactivés pour la vidéo {video_id}")
        else:
            print(f"Erreur API YouTube (get_video_comments): {e}")

def search_videos():
    query = random.choice(SEARCH_QUERIES)
    try:
        request = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=MAX_VIDEOS_PER_QUERY
        )
        response = request.execute()
        
        for item in response.get("items", []):
            video_id = item["id"]["videoId"]
            video_title = item["snippet"]["title"]
            print(f"Scan de la vidéo: {video_title}")
            get_video_comments(video_id, video_title)
            
    except Exception as e:
        print(f"Erreur API YouTube (search_videos): {e}")


# Boucle principale pour simuler un flux
print("Démarrage du producteur YouTube (simulation de flux)...")
try:
    while True:
        search_videos()
        print("Cycle de recherche terminé. En pause pour 60 secondes...")
        time.sleep(60) # Pause pour simuler un flux et respecter les quotas
except KeyboardInterrupt:
    print("Arrêt du producteur YouTube.")
finally:
    producer.close()