
# ============================================
# MODULE 6: Bronze Layer - Payments Ingestion
# Description: Payment transactions with partitioning
# ============================================

def ingest_payments_to_bronze():
    """
    Ingest payment data with date partitioning for performance
    """
    
    payments_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "avro")
        .option("cloudFiles.schemaLocation", f"{config['checkpoint_base']}/payments/schema")
        .load(f"{config['mount_point_bronze']}/raw_landing/payments/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_payment_date", to_date(col("order_purchase_timestamp")))
    )
    
    # Clean payment data
    cleaned_payments = (payments_stream
        .withColumn("payment_value", 
            when(col("payment_value").isNull() | col("payment_value") < 0, 0.0)
            .otherwise(col("payment_value")))
        .withColumn("payment_type",
            when(col("payment_type").isNull(), "not_defined")
            .otherwise(lower(trim(col("payment_type")))))
        .filter(col("order_id").isNotNull())
    )
    
    query = (cleaned_payments.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_base']}/payments/write_checkpoint")
        .partitionBy("_payment_date")  # Partition by date
        .trigger(processingTime="5 minutes")
        .table(f"{config['database_bronze']}.payments_bronze")
    )
    
    return query

payments_query = ingest_payments_to_bronze()