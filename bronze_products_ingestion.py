
# ============================================
# MODULE 4: Bronze Layer - Products Raw Ingestion
# Description: Products catalog ingestion with validation
# ============================================

def ingest_products_to_bronze():
    """
    Ingest product catalog with basic validation
    Handles schema evolution automatically
    """
    
    products_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{config['checkpoint_base']}/products/schema")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("badRecordsPath", f"{config['mount_point_bronze']}/bad_records/products/")
        .load(f"{config['mount_point_bronze']}/raw_landing/products/")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    # Validate required fields
    validated_stream = (products_stream
        .filter(col("product_id").isNotNull())
        .filter(col("product_category_name").isNotNull())
        .withColumn("_validation_status", 
            when(col("product_id").isNull() | col("product_category_name").isNull(), "FAILED")
            .otherwise("PASSED"))
    )
    
    query = (validated_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config['checkpoint_base']}/products/write_checkpoint")
        .trigger(processingTime="5 minutes")
        .table(f"{config['database_bronze']}.products_bronze")
    )
    
    return query

products_query = ingest_products_to_bronze()