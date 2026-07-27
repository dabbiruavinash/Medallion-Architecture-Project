
# ============================================
# MODULE 13: Gold Layer - Customer Segmentation
# Description: Creates customer segments based on 
#              purchasing behavior using RFM analysis
# ============================================

def create_customer_segmentation():
    """
    Gold layer: Customer segmentation using RFM analysis
    Recency, Frequency, Monetary value based segments
    """
    
    # Get max process date
    try:
        max_date = spark.sql(f"""
            SELECT CAST(COALESCE(MAX(segment_date), '1970-01-01') AS STRING) as max_date
            FROM {config['database_gold']}.customer_segments
        """).collect()[0]['max_date']
    except:
        max_date = '1970-01-01'
    
    # RFM Calculation
    customer_rfm = spark.sql(f"""
        WITH customer_purchases AS (
            SELECT 
                o.customer_id,
                MAX(DATE(o.order_purchase_timestamp)) as last_purchase_date,
                COUNT(DISTINCT o.order_id) as total_orders,
                SUM(oi.price) as total_spent,
                AVG(oi.price) as avg_order_value,
                DATEDIFF(CURRENT_DATE(), MAX(DATE(o.order_purchase_timestamp))) as days_since_last_purchase
            FROM {config['database_silver']}.orders_silver o
            INNER JOIN {config['database_silver']}.order_items_silver oi
                ON o.order_id = oi.order_id
            WHERE DATE(o.order_purchase_timestamp) > '{max_date}'
            GROUP BY o.customer_id
        )
        SELECT 
            customer_id,
            last_purchase_date,
            total_orders,
            total_spent,
            avg_order_value,
            days_since_last_purchase,
            CASE 
                WHEN days_since_last_purchase <= 30 THEN 5
                WHEN days_since_last_purchase <= 60 THEN 4
                WHEN days_since_last_purchase <= 90 THEN 3
                WHEN days_since_last_purchase <= 180 THEN 2
                ELSE 1
            END as recency_score,
            CASE 
                WHEN total_orders >= 10 THEN 5
                WHEN total_orders >= 7 THEN 4
                WHEN total_orders >= 5 THEN 3
                WHEN total_orders >= 3 THEN 2
                ELSE 1
            END as frequency_score,
            CASE 
                WHEN total_spent >= 50000 THEN 5
                WHEN total_spent >= 30000 THEN 4
                WHEN total_spent >= 10000 THEN 3
                WHEN total_spent >= 5000 THEN 2
                ELSE 1
            END as monetary_score
        FROM customer_purchases
    """)
    
    # Add segmentation
    customer_segments = (customer_rfm
        .withColumn("rfm_score", 
            col("recency_score") + col("frequency_score") + col("monetary_score"))
        .withColumn("customer_segment",
            when(col("rfm_score") >= 13, "CHAMPIONS")
            .when(col("rfm_score") >= 10, "LOYAL_CUSTOMERS")
            .when(col("rfm_score") >= 7, "POTENTIAL_LOYALISTS")
            .when(col("rfm_score") >= 5, "AT_RISK")
            .otherwise("LOST"))
        .withColumn("segment_date", current_date())
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.customer_segments (
            customer_id INT,
            last_purchase_date DATE,
            total_orders BIGINT,
            total_spent DOUBLE,
            avg_order_value DOUBLE,
            days_since_last_purchase INT,
            recency_score INT,
            frequency_score INT,
            monetary_score INT,
            rfm_score INT,
            customer_segment STRING,
            segment_date DATE,
            _processed_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (segment_date)
        LOCATION '{config['mount_point_gold']}/customer_segments'
    """)
    
    # Write to gold
    (customer_segments.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{config['database_gold']}.customer_segments")
    )
    
    # Log segmentation distribution
    print("Customer Segment Distribution:")
    customer_segments.groupBy("customer_segment").count().show()
    
    return customer_segments

# Execute customer segmentation
create_customer_segmentation()