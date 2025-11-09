import requests
from kafka import KafkaProducer
import json
import time
import sys
import os


USER_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "REMPLACEZ_PAR_VOTRE_JETON_DE_60_JOURS")
THREADS_USER_ID = os.getenv("THREADS_USER_ID", "REMPLACEZ_PAR_VOTRE_ID_UTILISATEUR_THREADS") 


TARGET_THREAD_ID = os.getenv("TARGET_THREAD_ID", "1234567890123456789")



if USER_ACCESS_TOKEN == "REMPLACEZ_PAR_VOTRE_JETON_DE_60_JOURS" or \
   THREADS_USER_ID == "REMPLACEZ_PAR_VOTRE_ID_UTILISATEUR_THREADS":
    print("ERREUR: Veuillez configurer USER_ACCESS_TOKEN et THREADS_USER_ID dans les variables d'environnement ou dans ce script.")
    sys.exit(1)



KAFKA_TOPIC = "raw_threads_comments"
KAFKA_SERVER = "kafka:29092"
POLLING_INTERVAL = 30 


API_ENDPOINT = f"https://graph.threads.net/v1.0/{TARGET_THREAD_ID}/replies"


FIELDS = "id,text_content,timestamp,user,parent_thread" 

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
except Exception as e:
    print(f"Erreur de connexion Kafka: {e}")
    sys.exit(1)

print(f"Connexion Kafka établie. Streaming des réponses du Thread ID: {TARGET_THREAD_ID}")


processed_ids = set()

def fetch_threads_replies():
    """Effectue une requête à l'API Threads pour obtenir les réponses."""
    params = {
        "access_token": USER_ACCESS_TOKEN,
        "fields": FIELDS,
        "limit": 100 
    }

    try:
        response = requests.get(API_ENDPOINT, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"Erreur HTTP (Rate Limit ?): {err}. Attente avant de réessayer.")
        
        return None
    except Exception as e:
        print(f"Erreur lors de la requête API Threads: {e}")
        return None

try:
    while True:
        data = fetch_threads_replies()
        
        if data and 'data' in data:
            new_comments_count = 0
            
            
            for reply in data['data']:
                comment_id = reply.get('id')
                
                
                if comment_id and comment_id not in processed_ids:
                    
                    
                    harmonized_data = {
                        
                        "id": comment_id,
                        "source": "threads",
                        "text_content": reply.get('text_content', ''),
                        
                        "timestamp": int(reply.get('timestamp')),
                        
                        "author": reply.get('user', {}).get('id', 'unknown'), 
                        
                        "metadata": {
                            
                            "source_specific_id": "threads", 
                            "post_id": TARGET_THREAD_ID,
                            "post_title": f"Reply to Thread {TARGET_THREAD_ID}"
                        }
                    }
                    
                    print(f"[THREADS] -> {harmonized_data['metadata']['post_id']}: {harmonized_data['text_content'][:50]}...")
                    producer.send(KAFKA_TOPIC, value=harmonized_data)
                    
                    processed_ids.add(comment_id)
                    new_comments_count += 1
            
            print(f"Lot traité. {new_comments_count} nouveaux commentaires envoyés à Kafka. Total traités: {len(processed_ids)}")
        
        
        time.sleep(POLLING_INTERVAL)

except KeyboardInterrupt:
    print("Arrêt du producteur Threads.")
except Exception as e:
    print(f"Erreur critique de stream Threads: {e}")
finally:
    producer.close()