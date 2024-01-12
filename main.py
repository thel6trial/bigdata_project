from consumer import Consumer
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, when, from_json, col
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)
from datetime import datetime

load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER")
KAFKA_TOPIC_NAME = os.getenv("KAFKA_TOPIC_NAME")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")


SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")
MONGO_USERNAME = os.getenv("MONGO_USERNAME")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME")
# MONGO_CONNECTION_STRING = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/{MONGO_DATABASE_NAME}.device"
COLLECTION = "netflix"
MONGO_CONNECTION_STRING = (
    f"mongodb://localhost:27017/{MONGO_DATABASE_NAME}.{COLLECTION}"
)


packages = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3",
    "org.elasticsearch:elasticsearch-spark-30_2.12:7.17.16",
    "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2",
]

spark = (
    SparkSession.builder.master("local")
    .config("spark.jars.packages", ",".join(packages))
    .appName("spark")
    .config("spark.executor.memory", "1g")
    .config("spark.cores.max", "1")
    .config("spark.mongodb.input.uri", MONGO_CONNECTION_STRING)
    .config("spark.mongodb.output.uri", MONGO_CONNECTION_STRING)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("Error")

index_name = "weather"


def writeToElasticsearch(df, epoch_id):
    df.show(truncate=False)
    df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", "http://localhost:9200"
    ).option("es.mapping.id", "id").option("es.resource", "index/type").option(
        "es.write.operation", "upsert"
    ).mode(
        "append"
    ).save()


spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from testtopic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
    .option("subscribe", KAFKA_TOPIC_NAME)
    .option("startingOffsets", "latest")
    .load()
)

df.printSchema()


df1 = df.selectExpr("CAST(value AS STRING)", "timestamp")

schema = StructType(
    [
        # StructField("day", StringType(), True),
        StructField("id", IntegerType(), False),
        StructField("weather_code", StringType(), True),
        StructField("temperature_2m_max", DoubleType(), True),
        StructField("temperature_2m_min", DoubleType(), True),
        StructField("temperature_2m_mean", DoubleType(), True),
        StructField("apparent_temperature_max", DoubleType(), True),
        StructField("apparent_temperature_min", DoubleType(), True),
        StructField("apparent_temperature_mean", DoubleType(), True),
        StructField("daylight_duration", DoubleType(), True),
        StructField("sunshine_duration", DoubleType(), True),
        StructField("precipitation_sum", DoubleType(), True),
        StructField("rain_sum", DoubleType(), True),
        StructField("snowfall_sum", DoubleType(), True),
        StructField("precipitation_hours", DoubleType(), True),
        StructField("wind_speed_10m_max", DoubleType(), True),
        StructField("wind_gusts_10m_max", DoubleType(), True),
        StructField("wind_direction_10m_dominant", DoubleType(), True),
        StructField("shortwave_radiation_sum", DoubleType(), True),
    ]
)

df2 = df1.select(from_json(col("value"), schema=schema).alias("weather"), "timestamp")

df3 = df2.select("weather.*")

df3.printSchema()

query = df3.writeStream.outputMode("append").foreachBatch(writeToElasticsearch).start()
# .option("es.nodes", "http://localhost:9200") \
# .option("checkpointLocation", "/tmp/") \
# .option("es.mapping.id", "id") \
# .option("es.resource", "index/type") \
# .format("console") \
# .format("org.elasticsearch.spark.sql") \

query.awaitTermination()
