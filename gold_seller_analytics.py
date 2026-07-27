
# ============================================
# MODULE 15: Gold Layer - Seller Performance Analytics
# Description: Comprehensive seller metrics and rankings
# ============================================

def create_seller_analytics():
    """
    Gold layer: Seller performance including:
    - Sales metrics
    - Delivery performance
    - Customer satisfaction
    """
    
    # Incremental processing timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_gold']}.seller_analytics
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    seller_analytics = spark.sql(f"""
        WITH seller_orders AS (
            SELECT 
                oi.seller_id,
                COUNT(DISTINCT oi.order_id) as total_orders,
                COUNT(DISTINCT oi.product_id) as unique_products_sold,
                COUNT(DISTINCT o.customer_id) as unique_customers,
                SUM(oi.price) as total_revenue,
                AVG(oi.price) as avg_order_value,
                SUM(oi.freight_value) as total_freight_collected,
                AVG(oi.freight_ratio) as avg_freight_ratio,
                AVG(DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp)) as avg_delivery_days,
                SUM(CASE WHEN o.is_delayed = true THEN 1 ELSE 0 END) as delayed_deliveries,
                MIN(o.order_purchase_timestamp) as first_sale_date,
                MAX(o.order_purchase_timestamp) as last_sale_date
            FROM {config['database_silver']}.order_items_silver oi
            INNER JOIN {config['database_silver']}.orders_silver o
                ON oi.order_id = o.order_id
            WHERE oi._processed_timestamp > '{max_timestamp}'
            GROUP BY oi.seller_id
        )
        SELECT 
            seller_id,
            total_orders,
            unique_products_sold,
            unique_customers,
            total_revenue,
            avg_order_value,
            total_freight_collected,
            avg_freight_ratio,
            avg_delivery_days,
            delayed_deliveries,
            ROUND(delayed_deliveries * 100.0 / NULLIF(total_orders, 0), 2) as delayed_rate,
            DATEDIFF(last_sale_date, first_sale_date) as active_days,
            ROUND(total_revenue / NULLIF(DATEDIFF(last_sale_date, first_sale_date), 0), 2) as daily_revenue,
            first_sale_date,
            last_sale_date
        FROM seller_orders
    """)
    
    # Add seller tier and performance indicators
    window_spec = Window.orderBy(desc("total_revenue"))
    
    seller_analytics_with_tiers = (seller_analytics
        .withColumn("seller_rank", row_number().over(window_spec))
        .withColumn("seller_tier",
            when(col("seller_rank") <= 10, "ELITE")
            .when(col("seller_rank") <= 50, "TOP_PERFORMER")
            .when(col("seller_rank") <= 200, "GROWING")
            .when(col("seller_rank") <= 500, "ESTABLISHED")
            .otherwise("NEW"))
        .withColumn("reliability_score",
            when(col("delayed_rate") <= 5, 5)
            .when(col("delayed_rate") <= 10, 4)
            .when(col("delayed_rate") <= 20, 3)
            .when(col("delayed_rate") <= 30, 2)
            .otherwise(1))
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.seller_analytics (
            seller_id STRING,
            total_orders BIGINT,
            unique_products_sold BIGINT,
            unique_customers BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            total_freight_collected DOUBLE,
            avg_freight_ratio DOUBLE,
            avg_delivery_days DOUBLE,
            delayed_deliveries BIGINT,
            delayed_rate DOUBLE,
            active_days INT,
            daily_revenue DOUBLE,
            first_sale_date TIMESTAMP,
            last_sale_date TIMESTAMP,
            seller_rank BIGINT,
            seller_tier STRING,
            reliability_score INT,
            _processed_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (seller_tier)
        LOCATION '{config['mount_point_gold']}/seller_analytics'
    """)
    
    # Merge using temporal logic
    seller_analytics_with_tiers.createOrReplaceTempView("new_seller_analytics")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.seller_analytics AS target
        USING new_seller_analytics AS source
        ON target.seller_id = source.seller_id
        
        WHEN MATCHED AND target.last_sale_date < source.last_sale_date THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    return seller_analytics_with_tiers

# Execute seller analytics
create_seller_analytics()