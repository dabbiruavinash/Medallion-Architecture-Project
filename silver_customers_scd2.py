
# ============================================
# MODULE 8: Silver Layer - Customers SCD Type 2
# Description: Implements Slowly Changing Dimension Type 2
#              Tracks all historical changes to customer data
# ============================================

def process_customers_scd2():
    """
    SCD Type 2 Implementation for Customers:
    - Tracks all changes over time
    - Uses effective_date and end_date
    - Maintains current flag
    - Preserves full history
    """
    
    # Get current date for SCD tracking
    current_date = current_timestamp()
    
    # Read bronze customer data
    bronze_customers = spark.sql(f"""
        SELECT 
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            customer_phone,
            customer_email,
            customer_created_at,
            _ingestion_timestamp as updated_at
        FROM {config['database_bronze']}.customers_bronze
    """)
    
    # Create SCD Type 2 Silver table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_silver']}.customers_scd2 (
            customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
            customer_id STRING,
            customer_unique_id STRING,
            customer_zip_code_prefix INT,
            customer_city STRING,
            customer_state STRING,
            customer_phone STRING,
            customer_email STRING,
            customer_created_at TIMESTAMP,
            effective_start_date TIMESTAMP,
            effective_end_date TIMESTAMP,
            is_current BOOLEAN,
            version INT,
            _processed_timestamp TIMESTAMP,
            _hash_key STRING
        )
        USING DELTA
        LOCATION '{config['mount_point_silver']}/customers_scd2'
    """)
    
    # Prepare source data with hash for change detection
    source_data = (bronze_customers
        .withColumn("_hash_key", 
            sha2(concat_ws("||",
                col("customer_unique_id"),
                col("customer_zip_code_prefix"),
                col("customer_city"),
                col("customer_state"),
                col("customer_phone"),
                col("customer_email")
            ), 256))
        .withColumn("_processed_timestamp", current_date)
    )
    
    # Get existing active records from silver
    existing_active = spark.sql(f"""
        SELECT 
            customer_sk,
            customer_id,
            _hash_key,
            version,
            effective_start_date
        FROM {config['database_silver']}.customers_scd2
        WHERE is_current = true
    """)
    
    # Detect changes using hash comparison
    changes_detected = (source_data.alias("src")
        .join(existing_active.alias("tgt"), "customer_id", "left")
        .filter(
            (col("tgt._hash_key").isNull()) |  # New record
            (col("src._hash_key") != col("tgt._hash_key"))  # Changed record
        )
        .select(
            col("src.customer_id"),
            col("src.customer_unique_id"),
            col("src.customer_zip_code_prefix"),
            col("src.customer_city"),
            col("src.customer_state"),
            col("src.customer_phone"),
            col("src.customer_email"),
            col("src.customer_created_at"),
            current_date.alias("effective_start_date"),
            lit(None).cast(TimestampType()).alias("effective_end_date"),
            lit(True).alias("is_current"),
            coalesce(col("tgt.version"), lit(0)).cast("int") + 1,
            col("src._processed_timestamp"),
            col("src._hash_key")
        )
    )
    
    # Update existing records (close them)
    records_to_close = (changes_detected
        .select("customer_id")
        .distinct()
        .join(existing_active, "customer_id")
        .select(col("customer_sk"))
    )
    
    if records_to_close.count() > 0:
        records_to_close.createOrReplaceTempView("records_to_close")
        
        close_query = f"""
            MERGE INTO {config['database_silver']}.customers_scd2 AS target
            USING records_to_close AS source
            ON target.customer_sk = source.customer_sk
            WHEN MATCHED THEN
                UPDATE SET
                    effective_end_date = current_timestamp(),
                    is_current = false
        """
        spark.sql(close_query)
    
    # Insert new/updated records
    (changes_detected.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{config['database_silver']}.customers_scd2")
    )
    
    print(f"SCD Type 2 processing complete. Records processed: {changes_detected.count()}")
    
    return changes_detected

# Execute SCD Type 2 process
process_customers_scd2()