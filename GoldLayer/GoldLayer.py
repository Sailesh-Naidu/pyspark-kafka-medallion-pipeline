from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window,sum,count,avg,expr, round

def create_spark():
    return (SparkSession.builder
             .appName("GoldLayer").config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0"
    ).config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .master("local[*]")
             .getOrCreate())

# Read validated streaming data from Silver layer
def read_silver_data(spark):
    return spark.readStream.format("delta").load("../silverLayer/data/silver/trades")

def aggregate_gold_data(silver_data):
    # Apply watermark to manage late-arriving data and state cleanup
    watermarked_df = silver_data.withWatermark("trade_timestamp", "10 minutes")
    watermarked_df = watermarked_df.select("trade_id", "trader_id", "quantity", "price", "trade_timestamp")

    # Aggregate trader metrics over 15-minute event-time windows
    return (watermarked_df
           .groupBy(window("trade_timestamp","15 minutes"), col("trader_id"))
           .agg(
                sum("quantity").alias('total_quantity'),
                count("trade_id").alias('trade_count'),
                round(avg("price"), 2).alias('avg_price'),
                round(sum(expr("price * quantity")),2).alias("total_trade_value")
))

def write_stream(gold_df_trades):
    return (gold_df_trades.writeStream.format("delta").outputMode("append")
         .trigger(processingTime="10 seconds",)
         .option("checkpointLocation", "checkpoints/gold/trader_metrics")
         .option("path", "data/gold/trader_metrics")
         .queryName("gold_trader_metrics")
         .start())

def main():
    spark = create_spark()

    spark.sparkContext.setLogLevel("ERROR")

    silver_data = read_silver_data(spark)

    gold_df_trades = aggregate_gold_data(silver_data)

    query = write_stream(gold_df_trades)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()





