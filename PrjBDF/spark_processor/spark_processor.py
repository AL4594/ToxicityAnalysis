from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, lit, struct, to_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
from transformers import pipeline
import time

# --- Configuration ---
KAFKA_SERVER = "kafka:29092"
RAW_REDDIT_TOPIC = "raw_reddit_comments"
RAW_YOUTUBE_TOPIC = "raw_youtube_comments"
ANALYZED_TOPIC = "analyzed_comments"
SPARK_MASTER = "spark://spark-master:7077"

# --- Initialisation du modèle ML ---
# Utilisation d'un modèle plus léger pour l'inférence rapide
# Vous pouvez choisir un modèle de toxicité plus performant
# ex: "martin-ha/toxic-comment-model" ou "unitary/toxic-bert"
# Pour le français: "cmarkea/distilcamembert-base-sentiment"
print("Chargement du modèle de sentiment...")
# Ce modèle classe comme 'positive', 'negative', 'neutral'. 
# Nous allons mapper 'negative' comme 'toxique' pour simplifier.
sentiment_pipeline = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-eng")
print("Modèle chargé.")

def predict_toxicity(text):
    """
    UDF pour prédire la toxicité.
    Nous utilisons un modèle de sentiment et mappons NEGATIVE à un score de toxicité élevé.
    """
    if not text:
        return 0.0
    try:
        # Limiter la longueur du texte pour l'inférence
        result = sentiment_pipeline(text[:512])[0]
        if result['label'] == 'NEGATIVE':
            return float(result['score']) # Score de "négativité"
        else:
            return 1.0 - float(result['score']) # Score de "non-négativité"
    except Exception as e:
        print(f"Erreur d'inférence: {e}")
        return 0.0

# Enregistrer l'UDF
toxicity_udf = udf(predict_toxicity, StringType())

# --- Schémas des données Kafka ---
reddit_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("author", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("post_title", StringType(), True)
])

youtube_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("author", StringType(), True),
    StructField("video_id", StringType(), True),
    StructField("video_title", StringType(), True)
])

# --- Initialisation Spark ---
spark = SparkSession.builder \
    .appName("ToxicityAnalysis") \
    .master(SPARK_MASTER) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Session Spark créée.")

# --- Lecture des flux Kafka ---
df_reddit = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", RAW_REDDIT_TOPIC) \
    .load() \
    .select(from_json(col("value").cast("string"), reddit_schema).alias("data")) \
    .select("data.*") \
    .withColumn("source_name", col("subreddit")) # Pour l'agrégation

df_youtube = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", RAW_YOUTUBE_TOPIC) \
    .load() \
    .select(from_json(col("value").cast("string"), youtube_schema).alias("data")) \
    .select("data.*") \
    .withColumn("source_name", col("video_title")) # Pour l'agrégation

# --- Union des deux flux ---
df_combined = df_reddit.select("id", "source", "text_content", "timestamp", "author", "source_name") \
    .union(
        df_youtube.select("id", "source", "text_content", "timestamp", "author", "source_name")
    )

# --- Application du modèle ML ---
df_analyzed = df_combined \
    .withColumn("toxicity_score", toxicity_udf(col("text_content")).cast("float")) \
    .withColumn("processing_timestamp", lit(int(time.time())))

# --- Envoi des résultats vers Kafka (pour le consommateur Mongo) ---
# Nous transformons notre DataFrame en une seule colonne "value" au format JSON
query_kafka = df_analyzed \
    .select(to_json(struct("*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("topic", ANALYZED_TOPIC) \
    .option("checkpointLocation", "/tmp/checkpoints/kafka_writer") \
    .start()

# --- Requête de débogage (optionnel) ---
query_console = df_analyzed \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "true") \
    .start()

print("Streams démarrés. En attente de données...")
query_kafka.awaitTermination()
query_console.awaitTermination()