
# ============================================
# MODULE 7: Silver Layer - Orders with CDC
# Description: Implements Change Data Capture from Bronze to Silver
#              Using timestamp comparison and merge operations
# ============================================

def process_orders_bronze_to_silver():
    """
    CDC Implementation for Orders:
    1. Get max timestamp from silver
    2. Read new/changed records from bronze
    3. Apply deduplication and transformations
    4. Merge into silver table
    """
    
    # Get max timestamp from silver for incremental load
    try:
        max_timestamp_silver = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), CAST('1970-01-01' AS TIMESTAMP)) as max_ts
            FROM {config['database_silver']}.orders_silver
        """).collect()[0]['max_ts']
    except:
        max_timestamp_silver = '1970-01-01'
    
    print(f"Processing records after: {max_timestamp_silver}")
    
    # Read new records from bronze
    bronze_orders = spark.sql(f"""
        SELECT 
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            _ingestion_timestamp
        FROM {config['database_bronze']}.orders_bronze
        WHERE _ingestion_timestamp > '{max_timestamp_silver}'
    """)
    
    # Apply transformations
    silver_orders = (bronze_orders
        .withColumn("processing_time_days", 
            datediff("order_delivered_customer_date", "order_purchase_timestamp"))
        .withColumn("delivery_delay_days",
            when(col("order_delivered_customer_date").isNotNull() & 
                 col("order_estimated_delivery_date").isNotNull(),
                 datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
            .otherwise(None))
        .withColumn("is_delayed",
            when(col("delivery_delay_days") > 0, true).otherwise(false))
        .withColumn("order_year_month",
            date_format("order_purchase_timestamp", "yyyy-MM"))
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_source_system", lit("BRONZE_CDC"))
        .withColumn("_operation", lit("INSERT"))
    )
    
    # Create or update silver table with merge
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_silver']}.orders_silver (
            order_id BIGINT,
            customer_id INT,
            order_status STRING,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            payment_sequential INT,
            payment_type STRING,
            payment_installments INT,
            payment_value DOUBLE,
            processing_time_days INT,
            delivery_delay_days INT,
            is_delayed BOOLEAN,
            order_year_month STRING,
            _processed_timestamp TIMESTAMP,
            _source_system STRING,
            _operation STRING
        )
        USING DELTA
        PARTITIONED BY (order_year_month)
        LOCATION '{config['mount_point_silver']}/orders'
    """)
    
    # Register temp view for merge
    silver_orders.createOrReplaceTempView("orders_updates")
    
    # MERGE statement for CDC
    merge_query = f"""
        MERGE INTO {config['database_silver']}.orders_silver AS target
        USING orders_updates AS source
        ON target.order_id = source.order_id
        
        WHEN MATCHED AND (
            target.order_status != source.order_status OR
            target.order_delivered_customer_date != source.order_delivered_customer_date OR
            target.payment_value != source.payment_value
        ) THEN
            UPDATE SET
                order_status = source.order_status,
                order_delivered_customer_date = source.order_delivered_customer_date,
                payment_value = source.payment_value,
                processing_time_days = source.processing_time_days,
                delivery_delay_days = source.delivery_delay_days,
                is_delayed = source.is_delayed,
                _processed_timestamp = source._processed_timestamp,
                _operation = 'UPDATE'
                
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    # Log the processing
    processed_count = silver_orders.count()
    print(f"Processed {processed_count} records from Bronze to Silver")
    
    return processed_count

# Execute the CDC process
process_orders_bronze_to_silver()