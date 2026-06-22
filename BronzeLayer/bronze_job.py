from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.avro.functions import from_avro


def create_spark():
    return (
        SparkSession.builder.appName("BronzeLayer").config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-avro_2.12:3.5.0"
        ).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()
    )

def read_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "trades").option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

def read_avro(df):
    df = df.select(
        "topic",
        "partition",
        "offset",
        "value",
        expr("timestamp as kafka_timestamp"), ).withColumn("ingestion_time", current_timestamp())

    avro_df = df.select(
        expr("substring(value, 6, length(value)-5)").alias("avro_value"),
        "topic",
        "partition",
        "offset",
        "kafka_timestamp", "ingestion_time")

    with open("../schemas/trades.avsc", 'r') as f:
        avro_schema = f.read()

    return (avro_df.select(from_avro(col("avro_value"), avro_schema).alias("data"),
                               "topic",
                               "partition",
                               "offset",
                               "kafka_timestamp", "ingestion_time"))


def write_stream(df, checkpointlocation, path) :
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpointlocation)
        .option("path", path)
        .start()
    )

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    df = read_kafka(spark)
    avro_df = read_avro(df)

    query_trades = write_stream(avro_df,'checkpoints/bronze/trades', "data/bronze/trades")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()