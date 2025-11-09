import requests
from kafka import KafkaProducer
import json
import time
import sys
import os

# ATTENTION : L'API Threads est axée sur la gestion de contenu et non sur la recherche publique.
# Nous allons simuler un flux en lisant les réponses (replies) d'un Thread ID spécifique.

# --- Configuration API Threads ---
# CHARGEMENT DES JETONS DEPUIS LES VARIABLES D'ENVIRONNEMENT (RECOMMANDÉ)
# Remplacez les valeurs par défaut (après le 'os.getenv') par vos valeurs réelles
# APRÈS AVOIR OBTENU LE JETON DE LONGUE DURÉE (60 jours).
USER_ACCESS_TOKEN = os.getenv("THREADS_ACCESS_TOKEN", "REMPLACEZ_PAR_VOTRE_JETON_DE_60_JOURS")
THREADS_USER_ID = os.getenv("THREADS_USER_ID", "REMPLACEZ_PAR_VOTRE_ID_UTILISATEUR_THREADS") 

# Thread ID : C'est l'ID d'une publication (thread) spécifique dont vous voulez lire les commentaires.
# Vous devez remplacer ceci par un ID de thread réel auquel votre USER_ID a accès en lecture.
# Ce Thread ID peut être un post que vous avez créé vous-même via l'API, ou que l'utilisateur lié
# à l'application a créé. L'API est TRÈS restrictive.
TARGET_THREAD_ID = os.getenv("TARGET_THREAD_ID", "1234567890123456789")


# Vérification de la configuration critique
if USER_ACCESS_TOKEN == "REMPLACEZ_PAR_VOTRE_JETON_DE_60_JOURS" or \
   THREADS_USER_ID == "REMPLACEZ_PAR_VOTRE_ID_UTILISATEUR_THREADS":
    print("ERREUR: Veuillez configurer USER_ACCESS_TOKEN et THREADS_USER_ID dans les variables d'environnement ou dans ce script.")
    sys.exit(1)


# --- Configuration Kafka ---
KAFKA_TOPIC = "raw_threads_comments"
KAFKA_SERVER = "kafka:29092"
POLLING_INTERVAL = 30 # Intervalle de temps (secondes) entre deux requêtes API pour éviter le Rate Limit

# Point de terminaison pour obtenir les réponses (replies) d'un thread
# Note: L'API Threads ne fournit pas de stream en continu comme Reddit, d'où l'utilisation du Polling.
API_ENDPOINT = f"https://graph.threads.net/v1.0/{TARGET_THREAD_ID}/replies"

# Champs que nous voulons récupérer pour la réponse (reply)
# Note: Nous conservons les champs requis pour l'harmonisation des données.
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

# Variable pour stocker les IDs des commentaires déjà traités afin d'éviter les doublons
processed_ids = set()

def fetch_threads_replies():
    """Effectue une requête à l'API Threads pour obtenir les réponses."""
    params = {
        "access_token": USER_ACCESS_TOKEN,
        "fields": FIELDS,
        "limit": 100 # Récupère jusqu'à 100 réponses par appel
    }

    try:
        response = requests.get(API_ENDPOINT, params=params)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP (4xx ou 5xx)
        return response.json()
    except requests.exceptions.HTTPError as err:
        print(f"Erreur HTTP (Rate Limit ?): {err}. Attente avant de réessayer.")
        # Meta renvoie souvent 400, 403, ou 429 en cas de problème d'accès/limite
        return None
    except Exception as e:
        print(f"Erreur lors de la requête API Threads: {e}")
        return None

try:
    while True:
        data = fetch_threads_replies()
        
        if data and 'data' in data:
            new_comments_count = 0
            
            # L'API Threads renvoie les réponses sous la clé 'data'
            for reply in data['data']:
                comment_id = reply.get('id')
                
                # Vérifie si le commentaire a déjà été traité
                if comment_id and comment_id not in processed_ids:
                    
                    # Harmonisation des données au format attendu par votre processeur Spark
                    harmonized_data = {
                        # 'id' sera mappé à '_id' dans Spark
                        "id": comment_id,
                        "source": "threads",
                        "text_content": reply.get('text_content', ''),
                        # Le timestamp Meta est en secondes UNIX (int)
                        "timestamp": int(reply.get('timestamp')),
                        # L'API donne un objet 'user' contenant l'ID
                        "author": reply.get('user', {}).get('id', 'unknown'), 
                        # NOUVEAU: Imbrication des métadonnées comme dans le schéma MongoDB de Reddit
                        "metadata": {
                            # Utilisez un identifiant générique pour la source Threads
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
        
        # Attente pour éviter de dépasser les limites de taux (Rate Limits)
        time.sleep(POLLING_INTERVAL)

except KeyboardInterrupt:
    print("Arrêt du producteur Threads.")
except Exception as e:
    print(f"Erreur critique de stream Threads: {e}")
finally:
    producer.close()