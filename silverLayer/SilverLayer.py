from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, concat_ws, lit, when, col

def create_spark():
    return(
        SparkSession.builder
        .appName("SilverLayer")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0"
        )   .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
        .getOrCreate()
    )

def read_bronze(spark):
    return  spark.readStream.format("delta").load("/Users/saileshpola/PycharmProjects/PythonProject/PysparkKafkaETE/BronzeLayer/data/bronze/trades")

def transform(df):
    parsed_ts = to_timestamp(col("trade_timestamp"))

    trade_id_valid = col("trade_id").isNotNull()
    trader_id_valid = col("trader_id").isNotNull()
    price_type_valid = col("price").cast("double").isNotNull()
    price_value_valid = col("price") > 0
    quantity_type_valid = col("quantity").cast("int").isNotNull()
    quantity_value_valid = col("quantity") > 0
    trade_timestamp_valid = parsed_ts.isNotNull()
    trade_timestamp_not_future = parsed_ts <= col("ingestion_time")
    side_valid = col("side").isin(["BUY", "SELL"])
    exchange_valid = col("exchange").isin(["NYSE", "NASDAQ", "BINANCE"])

    valid_condition = (
            trade_id_valid &
            trader_id_valid &
            price_type_valid &
            price_value_valid &
            quantity_type_valid &
            quantity_value_valid &
            trade_timestamp_valid &
            trade_timestamp_not_future &
            side_valid &
            exchange_valid
    )

    valid_trades = df.filter(valid_condition)
    dlq_trades = (df.filter(~valid_condition).withColumn("dlq_reason", concat_ws(
        ", ",
        when(~trade_id_valid, lit("missing_trade_id")),
        when(~trader_id_valid, lit("missing_trader_id")),
        when(~price_type_valid, lit("invalid_price_type")),
        when(price_type_valid & ~price_value_valid, lit("invalid_price_value")),
        when(~quantity_type_valid, lit("invalid_quantity_type")),
        when(quantity_type_valid & ~quantity_value_valid, lit("invalid_quantity_value")),
        when(~trade_timestamp_valid, lit("invalid_trade_timestamp")),
        when(trade_timestamp_valid & ~trade_timestamp_not_future, lit("future_trade_timestamp")),
        when(~side_valid, lit("invalid_side")),
        when(~exchange_valid, lit("invalid_exchange"))
    ))
                  )

    valid_trades = valid_trades.withColumn(
        "trade_timestamp",
        parsed_ts
    )

    valid_trades = (
        valid_trades
        .withWatermark("trade_timestamp", "10 minutes")
        .dropDuplicates(["trade_id"])
    )

    return valid_trades, dlq_trades

def write_stream(df, checkpoint_location, path, query_name):
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("path", path)
        .queryName(query_name)
        .start()
    )



def main():
    spark = create_spark()

    spark.sparkContext.setLogLevel("ERROR")

    bronze_data = read_bronze(spark)

    valid_trades, dlq_trades = transform(bronze_data)

    query_valid = write_stream(
        valid_trades,
        'checkpoints/silver/trades',
        'data/silver/trades',
        'silver_trades'
    )

    query_dlq = write_stream(
        dlq_trades,
        'checkpoints/dlq/trades',
        'data/dlq/trades',
        'dlq_trades'
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
