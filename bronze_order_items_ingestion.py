
# ============================================
# MODULE 5: Bronze Layer - Order Items Ingestion
# Description: Order line items with foreign key validation
# ============================================

def ingest_order_items_to_bronze():
    """
    Ingest order items with enrichment from broadcast variables
    Using broadcast join for small dimension tables
    """
    
    # Broadcast product categories (small lookup table)
    product_categories = spark.table(f"{config['database_bronze']}.products_bronze") \
        .select("product_id", "product_category_name") \
        .distinct()
    
    broadcast_categories = broadcast(product_categories)
    
    order_items_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{config['checkpoint_base']}/order_items/schema")
        .load(f"{config['mount_point_bronze']}/raw_landing/order_items/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_batch_id", monotonically_increasing_id())
    )
    
    # Enrich with product category using broadcast join
    enriched_items = (order_items_stream
        .join(broadcast_categories, "product_id", "left")
        .withColumn("_enriched_category", col("product_category_name"))
    )
    
    query = (enriched_items.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_base']}/order_items/write_checkpoint")
        .trigger(processingTime="3 minutes")
        .table(f"{config['database_bronze']}.order_items_bronze")
    )
    
    return query

order_items_query = ingest_order_items_to_bronze()