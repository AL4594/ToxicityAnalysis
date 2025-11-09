from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType, MapType, FloatType
import pandas as pd


SPARK_APP_NAME = "SocialMediaToxicityProcessor"
KAFKA_SERVER = "kafka:29092"
KAFKA_TOPICS = "raw_reddit_comments,raw_youtube_comments" 
MONGO_OUTPUT_URI = "mongodb://mongo:27017/toxicity_db.raw_comments"
CHECKPOINT_LOCATION = "/tmp/spark_checkpoint/toxicity_processor_v2" 


spark = SparkSession \
    .builder \
    .appName(SPARK_APP_NAME) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("Spark Session Initialized.")


data_schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("text_content", StringType(), True),
    StructField("timestamp", LongType(), True), 
    StructField("author", StringType(), True),
    StructField("metadata", MapType(StringType(), StringType()), True) 
])



@pandas_udf(FloatType())
def analyze_toxicity(texts: pd.Series) -> pd.Series:
    """
    Applique un modèle de toxicité à un lot de textes (Pandas Series).
    """
    from transformers import pipeline
    
    
    toxicity_classifier = pipeline(
        "text-classification",
        model="unitary/toxic-bert", 
        framework="pt",
        device=-1 
    )

    results = toxicity_classifier(texts.tolist(), truncation=True, max_length=512, batch_size=16)
    
    def get_score(res):
        try:
            
            if res['label'].lower() == 'toxic' or res['label'] == 'LABEL_1':
                return float(res['score'])
            return float(1.0 - res['score']) 
        except:
            return 0.0 

    return pd.Series([get_score(r) for r in results])


kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPICS) \
    .option("startingOffsets", "latest") \
    .load()


data_df = kafka_stream \
    .select(from_json(col("value").cast("string"), data_schema).alias("data")) \
    .select("data.*")


processed_df = data_df.withColumn(
    "timestamp",
    col("timestamp").cast(TimestampType()) 
).withColumnRenamed(
    "id", "_id"
)


def foreach_batch_function(df, batchId):
    """Logique à exécuter pour chaque micro-lot de données."""
    if df.count() > 0:
        print(f"--- Démarrage du Traitement du Lot {batchId}. Taille: {df.count()} ---")
        
        df_analyzed = df \
            .filter(col("text_content").isNotNull() & (col("text_content") != "")) \
            .withColumn("toxicity_score", analyze_toxicity(col("text_content")))
        
        if df_analyzed.count() > 0:
            try:
                df_analyzed.write \
                    .format("mongo") \
                    .mode("append") \
                    .option("uri", MONGO_OUTPUT_URI) \
                    .option("spark.mongodb.output.writeConcern.w", "majority") \
                    .save()
                print(f"Lot {batchId} analysé et écrit avec succès dans MongoDB.")
            except Exception as e:
                print(f"Erreur d'écriture MongoDB pour le lot {batchId}: {e}")
                
                print("--- TRACE D'ERREUR PYTHON DU WORKER ---")
                
                if hasattr(e, 'cause') and e.cause is not None:
                    print(e.cause)
                print("-------------------------------------")
        else:
            print(f"Lot {batchId} ne contenait aucun texte valide après filtrage.")
            
    else:
        print(f"Lot {batchId} vide.")



query = processed_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .start()

print("Spark Streaming Query Started. Listening to Kafka topics...")


query.awaitTermination()