from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# --- Initialisation Spark Session ---
# Les packages sont inclus via le docker-compose
spark = SparkSession \
    .builder \
    .appName("ToxicityStreamProcessorRedditOnly") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session démarrée (Mode Reddit Uniquement).")

# --- Schéma des données entrantes de Kafka (issu de reddit_producer.py) ---
# Nous incluons uniquement les champs que nous recevons de Reddit
base_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True), # Sera toujours "reddit"
    StructField("text_content", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("post_id", StringType(), True),
    StructField("post_title", StringType(), True)
])

# --- Fonction de traitement par lot (Écriture dans Mongo) ---
def process_stream_and_store(df, batchId):
    """Consomme les données de Kafka, les harmonise et les stocke dans Mongo."""
    print(f"--- Traitement du lot {batchId} ({df.count()} lignes) ---")
    
    if df.count() == 0:
        print("Lot vide. Saut.")
        return
    
    # 1. Désérialisation du JSON
    value_df = df.select(
        from_json(col("value").cast("string"), base_schema).alias("data")
    ).select("data.*")

    # 2. Harmonisation de la structure pour MongoDB (Simplifié pour Reddit)
    processed_df = value_df \
        .withColumn("_id", col("id")) \
        .withColumn("ingestion_time", current_timestamp()) \
        .withColumn("timestamp_utc", col("timestamp").cast(TimestampType())) \
        .withColumn(
            "metadata", 
            # On crée un objet metadata spécifique à Reddit
            struct(
                col("subreddit").alias("source_specific_id"), 
                col("post_id"), 
                col("post_title")
            )
        ) \
        .select(
            col("_id"), 
            col("source"), 
            col("text_content"), 
            col("timestamp_utc").alias("timestamp"), 
            col("author"), 
            col("metadata"), 
            col("ingestion_time")
        )
    
    # 3. Écriture dans MongoDB
    MONGO_OUTPUT_URI = "mongodb://mongo:27017/toxicity_db.raw_comments"
    
    try:
        processed_df.write \
            .format("mongo") \
            .mode("append") \
            .option("uri", MONGO_OUTPUT_URI) \
            .save()
        print(f"Lot {batchId} écrit avec succès dans MongoDB (collection raw_comments).")
    except Exception as e:
        print(f"Erreur d'écriture MongoDB pour le lot {batchId}: {e}")

# --- Démarrage du Spark Streaming pour le topic Reddit uniquement ---
KAFKA_REDDIT_TOPIC = "raw_reddit_comments" # Changement ici

# Configuration du streaming Kafka pour le topic Reddit
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", KAFKA_REDDIT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Lancer la requête de streaming
query = kafka_stream_df \
    .writeStream \
    .foreachBatch(process_stream_and_store) \
    .option("checkpointLocation", "/tmp/spark_checkpoint/toxicity_processor_reddit") \
    .start()

query.awaitTermination()