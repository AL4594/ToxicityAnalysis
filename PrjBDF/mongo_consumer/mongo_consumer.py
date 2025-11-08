from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time
import sys

# --- Configuration ---
KAFKA_SERVER = "kafka:29092"
KAFKA_TOPIC = "analyzed_comments"
MONGO_SERVER = "mongo:27017"
MONGO_DB = "toxicity_db"
MONGO_COLLECTION = "comments"

print("Démarrage du consommateur Mongo...")

# Attendre que Kafka et Mongo soient prêts
time.sleep(30) 

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVER,
        auto_offset_reset='earliest',
        group_id='mongo-consumers',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("Connecté à Kafka.")
except Exception as e:
    print(f"Erreur connexion Kafka: {e}")
    sys.exit(1)

try:
    client = MongoClient(f"mongodb://{MONGO_SERVER}/")
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    # Créer un index pour accélérer les requêtes du dashboard
    collection.create_index([("toxicity_score", -1)])
    collection.create_index([("source", 1)])
    collection.create_index([("timestamp", -1)])
    print("Connecté à MongoDB.")
except Exception as e:
    print(f"Erreur connexion Mongo: {e}")
    sys.exit(1)

print("En attente de messages...")
try:
    for message in consumer:
        data = message.value
        try:
            # Insérer le document dans MongoDB
            collection.insert_one(data)
            print(f"MONGO INSERT -> Score: {data.get('toxicity_score', 'N/A')}, Source: {data.get('source_name', 'N/A')[:20]}...")
        except Exception as e:
            print(f"Erreur d'insertion Mongo: {e}")

except KeyboardInterrupt:
    print("Arrêt du consommateur Mongo.")
finally:
    consumer.close()
    client.close()