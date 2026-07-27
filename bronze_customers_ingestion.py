
# ============================================
# MODULE 3: Bronze Layer - Customers Raw Ingestion  
# Description: Ingests customer data with deduplication
#              using watermark and dropDuplicates
# ============================================

from pyspark.sql.window import Window

def ingest_customers_to_bronze():
    """
    Ingest customer data with deduplication logic
    Using watermark to handle updates within time window
    """
    
    customers_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", IntegerType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("customer_phone", StringType(), True),
        StructField("customer_email", StringType(), True),
        StructField("customer_created_at", TimestampType(), True),
        StructField("_last_updated", TimestampType(), True)
    ])
    
    customers_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{config['checkpoint_base']}/customers/schema")
        .option("multiLine", "false")
        .schema(customers_schema)
        .load(f"{config['mount_point_bronze']}/raw_landing/customers/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withWatermark("_last_updated", "48 hours")
    )
    
    # Deduplicate based on latest update
    window_spec = Window.partitionBy("customer_id").orderBy(desc("_last_updated"))
    
    deduped_stream = (customers_stream
        .withColumn("_row_number", row_number().over(window_spec))
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )
    
    query = (deduped_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_base']}/customers/write_checkpoint")
        .trigger(processingTime="10 minutes")
        .table(f"{config['database_bronze']}.customers_bronze")
    )
    
    return query

customers_query = ingest_customers_to_bronze()