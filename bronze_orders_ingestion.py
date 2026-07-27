
# ============================================
# MODULE 2: Bronze Layer - Orders Raw Ingestion
# Description: Ingests raw order data with watermark handling
#              for late-arriving data
# ============================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Define schema for orders
orders_schema = StructType([
    StructField("order_id", LongType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", TimestampType(), True),
    StructField("order_approved_at", TimestampType(), True),
    StructField("order_delivered_carrier_date", TimestampType(), True),
    StructField("order_delivered_customer_date", TimestampType(), True),
    StructField("order_estimated_delivery_date", TimestampType(), True),
    StructField("payment_sequential", IntegerType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_installments", IntegerType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("_ingestion_timestamp", TimestampType(), True),
    StructField("_source_file", StringType(), True)
])

# Read streaming/batch data with watermark
def ingest_orders_to_bronze():
    """
    Ingest raw orders data with watermark for late arrival handling
    Watermark: 24 hours tolerance for late data
    """
    
    # Read from raw landing zone
    orders_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{config['checkpoint_base']}/orders/schema")
        .option("cloudFiles.maxFilesPerTrigger", 1000)
        .option("header", "true")
        .option("mergeSchema", "true")
        .load(f"{config['mount_point_bronze']}/raw_landing/orders/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withWatermark("order_purchase_timestamp", "24 hours")  # Watermark for late data
    )
    
    # Write to bronze delta table
    query = (orders_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_base']}/orders/write_checkpoint")
        .trigger(processingTime="5 minutes")
        .table(f"{config['database_bronze']}.orders_bronze")
    )
    
    return query

# Start the streaming query
orders_query = ingest_orders_to_bronze()