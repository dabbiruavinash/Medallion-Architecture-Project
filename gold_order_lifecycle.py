
# ============================================
# MODULE 18: Gold Layer - Order Lifecycle Analytics
# Description: Track orders from creation to delivery
#              with bottleneck analysis
# ============================================

def create_order_lifecycle_analytics():
    """
    Gold layer: Complete order lifecycle tracking
    Identifies bottlenecks and SLA compliance
    """
    
    # Incremental processing with timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_gold']}.order_lifecycle
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    order_lifecycle = spark.sql(f"""
        SELECT 
            o.order_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            -- Lifecycle durations
            DATEDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at) as approval_time_hours,
            DATEDIFF(DAY, o.order_approved_at, o.order_delivered_carrier_date) as processing_time_days,
            DATEDIFF(DAY, o.order_delivered_carrier_date, o.order_delivered_customer_date) as shipping_time_days,
            DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) as total_cycle_time_days,
            -- SLA Compliance
            CASE 
                WHEN approval_time_hours <= 24 THEN 'WITHIN_SLA'
                WHEN approval_time_hours <= 48 THEN 'SLIGHT_DELAY'
                ELSE 'SLA_BREACH'
            END as approval_sla,
            -- Payment details
            p.payment_type_standardized,
            p.payment_installments,
            p.payment_value,
            -- Customer info
            c.customer_state,
            c.customer_city,
            -- Order value
            SUM(oi.price) as order_value,
            COUNT(DISTINCT oi.product_id) as items_count
        FROM {config['database_silver']}.orders_silver o
        INNER JOIN {config['database_silver']}.payments_silver p
            ON o.order_id = p.order_id
        INNER JOIN {config['database_silver']}.customers_scd2 c
            ON o.customer_id = c.customer_id AND c.is_current = true
        INNER JOIN {config['database_silver']}.order_items_silver oi
            ON o.order_id = oi.order_id
        WHERE o._processed_timestamp > '{max_timestamp}'
        GROUP BY o.order_id, o.order_status, o.order_purchase_timestamp,
                 o.order_approved_at, o.order_delivered_carrier_date,
                 o.order_delivered_customer_date, o.order_estimated_delivery_date,
                 p.payment_type_standardized, p.payment_installments, p.payment_value,
                 c.customer_state, c.customer_city
    """)
    
    # Add analytical columns
    lifecycle_with_analytics = (order_lifecycle
        .withColumn("is_completed",
            when(col("order_status") == "delivered", true).otherwise(false))
        .withColumn("bottleneck_stage",
            when(col("approval_time_hours") > 48, "APPROVAL")
            .when(col("processing_time_days") > 5, "PROCESSING")
            .when(col("shipping_time_days") > 7, "SHIPPING")
            .otherwise("NONE"))
        .withColumn("order_complexity",
            when(col("items_count") > 5, "COMPLEX")
            .when(col("items_count") > 2, "MODERATE")
            .otherwise("SIMPLE"))
        .withColumn("payment_complexity",
            when(col("payment_installments") > 5, "HIGH_INSTALLMENT")
            .when(col("payment_installments") > 1, "MODERATE_INSTALLMENT")
            .otherwise("SINGLE_PAYMENT"))
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.order_lifecycle (
            order_id BIGINT,
            order_status STRING,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            approval_time_hours BIGINT,
            processing_time_days INT,
            shipping_time_days INT,
            total_cycle_time_days INT,
            approval_sla STRING,
            payment_type_standardized STRING,
            payment_installments INT,
            payment_value DOUBLE,
            customer_state STRING,
            customer_city STRING,
            order_value DOUBLE,
            items_count BIGINT,
            is_completed BOOLEAN,
            bottleneck_stage STRING,
            order_complexity STRING,
            payment_complexity STRING,
            _processed_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (order_status, order_complexity)
        LOCATION '{config['mount_point_gold']}/order_lifecycle'
    """)
    
    # Write with merge
    lifecycle_with_analytics.createOrReplaceTempView("new_lifecycle_data")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.order_lifecycle AS target
        USING new_lifecycle_data AS source
        ON target.order_id = source.order_id
        
        WHEN MATCHED AND target.order_status != source.order_status THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    return lifecycle_with_analytics

# Execute order lifecycle analytics
create_order_lifecycle_analytics()