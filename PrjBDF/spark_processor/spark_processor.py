from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, MapType

# Configuration des applications Spark et des librairies
# Remarque : Les librairies 'spark-sql-kafka-0-10' et 'mongo-spark-connector'
# doivent être spécifiées dans le script de lancement (spark-submit)
# pour que cela fonctionne dans un environnement Docker.
SPARK_APP_NAME = "SocialMediaToxicityProcessor"
KAFKA_SERVER = "kafka:29092"
KAFKA_TOPICS = "raw_reddit_comments,raw_youtube_comments" # Listen to both topics
MONGO_OUTPUT_URI = "mongodb://mongo:27017/toxicity_db.raw_comments"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoint/toxicity_processor_v2" # Mise à jour du checkpoint pour les nouveaux topics

# 1. Initialisation de la session Spark
spark = SparkSession \
    .builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Initialized.")

# Schéma de décodage du message Kafka (clé, valeur, etc.)
kafka_schema = StructType([
    StructField("key", StringType(), True),
    StructField("value", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("timestamp", TimestampType(), True) # Timestamp Kafka
])

# Schéma des données internes (harmonisé pour Reddit, YouTube, Threads)
# Note: Nous utilisons LongType pour le timestamp UNIX brut
data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("timestamp", LongType(), True), # Timestamp UNIX (secondes)
    StructField("author", StringType(), True),
    # Le MapType gère l'objet imbriqué 'metadata' sans avoir besoin de définir tous ses sous-champs
    StructField("metadata", MapType(StringType(), StringType()), True) 
])

# 2. Lecture en Streaming depuis Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPICS) \
    .option("startingOffsets", "latest") \
    .load()

# 3. Transformation des données
data_df = kafka_stream \
    .select(from_json(col("value").cast("string"), data_schema).alias("data")) \
    .select("data.*")

# Harmonisation et conversions:
# a) Conversion du timestamp UNIX (secondes) en Spark/Mongo Timestamp (ISODate)
# b) Renommage de 'id' en '_id' pour la clé primaire MongoDB
# c) Ajout d'une colonne de traitement (optional)
processed_df = data_df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType()) # Convertit Long (secondes) en Timestamp pour MongoDB
).withColumnRenamed(
    "id", "_id"
)

# Affichage du schéma et du stream pour le debug
print("Schéma du DataFrame traité:")
processed_df.printSchema()

# 4. Fonction d'écriture par lot (pour Stuctured Streaming)
def foreach_batch_function(df, batchId):
    """Logique à exécuter pour chaque micro-lot de données."""
    if df.count() > 0:
        print(f"--- Démarrage du Traitement du Lot {batchId}. Taille: {df.count()} ---")
        
        # 5. Écriture dans MongoDB
        try:
            df.write \
                .format("mongo") \
                .mode("append") \
                .option("uri", MONGO_OUTPUT_URI) \
                .option("spark.mongodb.output.writeConcern.w", "majority") \
                .option("spark.mongodb.output.upsertDocument", "true") \
                .save()
            print(f"Lot {batchId} écrit avec succès dans MongoDB (collection raw_comments).")
        except Exception as e:
            print(f"Erreur d'écriture MongoDB pour le lot {batchId}: {e}")
    else:
        print(f"Lot {batchId} vide.")


# 6. Démarrage du stream d'écriture
query = processed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

print("Spark Streaming Query Started. Listening to Kafka topics...")

# 7. Attente de la fin du traitement (ctrl-C pour arrêter)
query.awaitTermination()