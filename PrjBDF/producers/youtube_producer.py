from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import time
import sys
import random

# --- Configuration YouTube (Utilisez votre clé !) ---
# !! ATTENTION : NE PAS EXPOSER CETTE CLÉ PUBLIQUEMENT !!
# REMPLACEZ CE JETON PAR VOTRE VRAIE CLÉ API GOOGLE
API_KEY = "AIzaSyBV_GSOqhCig17cgXwNFoEM0GGF9RU4nWM" # <-- CLÉ API GOOGLE CLOUD
SEARCH_QUERIES = ["débat politique", "actualités", "gaming review", "tech explained", "intelligence artificielle", "cuisine rapide"]
MAX_VIDEOS_PER_QUERY = 3 # Limite pour ne pas épuiser le quota
MAX_COMMENTS_PER_VIDEO = 5 # Limite pour ne pas épuiser le quota

# --- Configuration Kafka ---
KAFKA_TOPIC = "raw_youtube_comments"
KAFKA_SERVER = "kafka:29092"

# Initialisation
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    # Utilisation de la clé définie en haut du script
    youtube = build('youtube', 'v3', developerKey=API_KEY) 
    print("Connecté à Kafka et à l'API YouTube")
except Exception as e:
    # Affiche un message d'erreur si la clé n'est pas valide ou la connexion échoue
    print(f"Erreur de connexion: {e}") 
    sys.exit(1)

seen_comment_ids = set()

def get_video_comments(video_id, video_title):
    """Récupère les commentaires pour une vidéo donnée et les envoie à Kafka."""
    try:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=MAX_COMMENTS_PER_VIDEO,
            order="time" # Pour avoir les plus récents
        )
        response = request.execute()

        for item in response.get("items", []):
            comment_snippet = item["snippet"]["topLevelComment"]["snippet"]
            comment_id = item["snippet"]["topLevelComment"]["id"]
            
            if comment_id not in seen_comment_ids:
                # Harmonisation des données au format MongoDB compatible
                harmonized_data = {
                    "id": comment_id,
                    "source": "youtube",
                    "text_content": comment_snippet["textDisplay"],
                    # Conversion du format de date ISO en timestamp UNIX (secondes)
                    "timestamp": int(time.mktime(time.strptime(comment_snippet["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"))),
                    "author": comment_snippet.get("authorDisplayName", "unknown"),
                    # NOUVEAU: Imbrication des métadonnées comme dans le schéma MongoDB
                    "metadata": {
                        "source_specific_id": "youtube", 
                        "post_id": video_id,        # L'ID du post est l'ID de la vidéo
                        "post_title": video_title # Le titre du post est le titre de la vidéo
                    }
                }
                
                print(f"[YOUTUBE] -> {harmonized_data['metadata']['post_title'][:20]}...: {harmonized_data['text_content'][:50]}...")
                producer.send(KAFKA_TOPIC, value=harmonized_data)
                seen_comment_ids.add(comment_id)
                
    except Exception as e:
        if 'commenting is disabled' in str(e).lower():
            print(f"Commentaires désactivés pour la vidéo {video_id}")
        else:
            print(f"Erreur API YouTube (get_video_comments): {e}")

def search_videos():
    """Recherche des vidéos basées sur des requêtes aléatoires."""
    query = random.choice(SEARCH_QUERIES)
    print(f"\n--- Nouvelle recherche YouTube: '{query}' ---")
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