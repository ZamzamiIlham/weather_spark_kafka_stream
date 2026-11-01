import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, 
    max as _max, min as _min, avg as _avg, first, struct, to_json
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Konfigurasi Kafka
# Menggunakan 'kafka:9092' karena script ini dijalankan di dalam container Docker yang sama dengan network compose
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
INPUT_TOPIC = "weather-data"
VALID_OUTPUT_TOPIC = "weather-summary"
DLQ_TOPIC = "weather-data-dead-letter"

def get_spark_session():
    # Menyiapkan session Spark dengan package kafka-sql
    return SparkSession.builder \
        .appName("RealTimeWeatherETL") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()

def get_schema():
    return StructType([
        StructField("city", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("weather_code", IntegerType(), True),
        StructField("description", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def main():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Mulai membaca aliran data (stream) dari Kafka...")

    # 1. Data Collection & ETL: Membaca dari Kafka
    raw_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", INPUT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Data dari Kafka berformat binari, kita cast ke string lalu parse sebagai JSON
    schema = get_schema()
    parsed_stream = raw_stream.selectExpr("CAST(value AS STRING) as json_val") \
        .select(from_json(col("json_val"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    #cek valid angka
    valid_data = parsed_stream.filter(
        (col("temperature") > -50.0) & 
        (col("humidity") >= 0.0) & 
        (col("weather_code") >= 0)
    )

    #dlq data
    invalid_data = parsed_stream.filter(
        (col("temperature") <= -50.0) | 
        (col("humidity") < 0.0) | 
        (col("weather_code") < 0)
    )

    #agregasi 
    weather_summary = valid_data \
        .withWatermark("timestamp", "2 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            col("city")
        ) \
        .agg(
            _avg("temperature").alias("avg_temperature"),
            _max("temperature").alias("max_temperature"),
            _min("temperature").alias("min_temperature"),
            _avg("humidity").alias("avg_humidity"),
            first("description").alias("latest_description")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("avg_temperature"),
            col("max_temperature"),
            col("min_temperature"),
            col("avg_humidity"),
            col("latest_description")
        )

    #output sink
    kafka_output_df = weather_summary.selectExpr("to_json(struct(*)) AS value")

    summary_query= kafka_output_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", VALID_OUTPUT_TOPIC) \
        .option("checkpointLocation", "/tmp/checkpoints/weather_summary_v1") \
        .outputMode("append") \
        .start()

    #output sink untuk consol
    console_query = weather_summary \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .option("checkpointLocation", "/app/src/checkpoints/weather_console_v2") \
        .start()

    # Mengirim data yang invalid ke Dead Letter Queue Topic di Kafka
    dlq_kafka_df = invalid_data.selectExpr("to_json(struct(*)) AS value")
    dlq_query = dlq_kafka_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", DLQ_TOPIC) \
        .option("checkpointLocation", "/app/src/checkpoints/weather_dlq_v2") \
        .outputMode("append") \
        .start()

    print("Pipeline berjalan! Menunggu terminasi...")
    
    # Tunggu semua query berjalan tanpa henti
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
