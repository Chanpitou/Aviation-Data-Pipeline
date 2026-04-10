import os
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from pyspark.sql.functions import from_json, col, coalesce, lit
logging.basicConfig(format='%(asctime)s: %(levelname)s - %(message)s', level=logging.INFO)

load_dotenv()

# Create a spark session object
spark = (SparkSession.builder
    .appName("AviationStreaming")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.5," 
            "org.postgresql:postgresql:42.7.2")
    .getOrCreate())

# Specifying data types schema for incoming streaming data
aviation_schema = StructType([
    StructField("icao24", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("origin_country", StringType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("on_ground", BooleanType(), True),
    StructField("velocity", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])


def write_to_postgres(df, epoch_id):
    # Using the separate instance details
    df.write \
        .mode("append") \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5433/aviation_db") \
        .option("dbtable", "raw_flight_logs") \
        .option("user", "aviation_user") \
        .option("password", os.getenv("STREAM_POSTGRES_PASSWORD")) \
        .option("driver", "org.postgresql.Driver") \
        .save()

try:
    logging.info("Receiving incoming flight events...")
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "aviation_raw") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    logging.info("Cleaning/Transforming incoming flight events...")
    # Convert incoming data as string and apply data type schema
    parsed_df = raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"),aviation_schema).alias("data")).select("data.*")

    # Filter out any null latitude and longitude data
    cleaned_df = parsed_df.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    # Handle/Transform columns: callsign, velocity, baro-altitude
    final_df = cleaned_df.withColumn("callsign", coalesce(col("callsign"), lit("UNKNOWN"))) \
        .withColumn("velocity", coalesce(col("velocity"), lit(0.0))) \
        .withColumn("altitude", coalesce(col("altitude"), lit(0.0)))

    # snowflake_config = {
    #     "sfURL": os.getenv("SNOWFLAKE_URL"),
    #     "sfUser": os.getenv("SNOWFLAKE_USER"),
    #     "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    #     "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    #     "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    #     "sfRole": os.getenv("SNOWFLAKE_ROLE"),
    #     "pem_private_key": os.getenv("RSA_KEY"),
    #     "sfauthenticator": "snowflake_jwt"
    # }

    # query = final_df.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    # Start the stream
    query = final_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "checkpoints/aviation_postgres_v3") \
        .start()

    query.awaitTermination()

except Exception as e:
    logging.error(e)
