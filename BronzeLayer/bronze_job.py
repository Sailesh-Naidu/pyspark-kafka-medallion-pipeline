from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col


trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("trader_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("trade_timestamp", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("side", StringType(), True),
    StructField("metadata", StructType([
        StructField("source", StringType(), True),
        StructField("version", IntegerType(), True)
    ]), True),
])



def create_spark():
    return (
        SparkSession.builder.appName("BronzeLayer").config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "io.delta:delta-spark_2.12:3.1.0"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").master("local[*]").getOrCreate()
    )

def read_kafka(spark):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "trades").option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 100)
        .load()
    )

def transform(df):
    df1 = df.selectExpr("CAST(value AS STRING) as value",
                        "topic",
                        "partition",
                        "offset",
                        "timestamp as kafka_timestamp") \
        .withColumn("ingestion_time", current_timestamp())

    df_parsed = df1.withColumn('data', from_json('value', trade_schema, ))

    trades_bronze = df_parsed.select('data.*',
                                "topic",
                                "partition",
                                "offset",
                                "kafka_timestamp",
                                "ingestion_time",
                                )
    return trades_bronze

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
    trades_bronze = transform(df)

    query_trades = write_stream(trades_bronze,'checkpoints/bronze/trades', "data/bronze/trades")

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()