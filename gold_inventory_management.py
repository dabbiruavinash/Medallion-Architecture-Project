
# ============================================
# MODULE 16: Gold Layer - Inventory Analytics
# Description: Inventory turnover, stock predictions
#              and reorder recommendations
# ============================================

def create_inventory_analytics():
    """
    Gold layer: Inventory management metrics
    Uses max(timestamp) for incremental processing
    """
    
    # Get max processing timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_gold']}.inventory_analytics
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    inventory_analytics = spark.sql(f"""
        WITH product_movement AS (
            SELECT 
                oi.product_id,
                p.product_category_name_english as category,
                DATE_TRUNC('month', o.order_purchase_timestamp) as sale_month,
                COUNT(DISTINCT oi.order_id) as monthly_orders,
                SUM(oi.price) as monthly_revenue,
                COUNT(DISTINCT o.customer_id) as monthly_buyers
            FROM {config['database_silver']}.order_items_silver oi
            INNER JOIN {config['database_silver']}.orders_silver o
                ON oi.order_id = o.order_id
            INNER JOIN {config['database_silver']}.products_silver p
                ON oi.product_id = p.product_id
            WHERE oi._processed_timestamp > '{max_timestamp}'
            GROUP BY oi.product_id, p.product_category_name_english, 
                     DATE_TRUNC('month', o.order_purchase_timestamp)
        ),
        inventory_stats AS (
            SELECT 
                product_id,
                category,
                COUNT(DISTINCT sale_month) as months_active,
                SUM(monthly_orders) as total_orders,
                AVG(monthly_orders) as avg_monthly_orders,
                STDDEV(monthly_orders) as stddev_monthly_orders,
                MAX(monthly_orders) as peak_monthly_orders,
                MIN(monthly_orders) as low_monthly_orders,
                SUM(monthly_revenue) as total_revenue,
                AVG(monthly_revenue) as avg_monthly_revenue,
                SUM(monthly_buyers) as total_buyers
            FROM product_movement
            GROUP BY product_id, category
        )
        SELECT 
            product_id,
            category,
            months_active,
            total_orders,
            avg_monthly_orders,
            stddev_monthly_orders,
            peak_monthly_orders,
            low_monthly_orders,
            total_revenue,
            avg_monthly_revenue,
            total_buyers,
            ROUND(total_orders / NULLIF(months_active, 0), 2) as inventory_turnover_rate,
            ROUND(avg_monthly_orders + (2 * COALESCE(stddev_monthly_orders, 0)), 0) as recommended_reorder_level,
            CASE 
                WHEN inventory_turnover_rate > 100 THEN 'FAST_MOVING'
                WHEN inventory_turnover_rate > 50 THEN 'REGULAR'
                WHEN inventory_turnover_rate > 10 THEN 'SLOW_MOVING'
                ELSE 'DEAD_STOCK'
            END as inventory_category
        FROM inventory_stats
    """)
    
    # Add metadata
    inventory_with_meta = (inventory_analytics
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_analysis_date", current_date())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.inventory_analytics (
            product_id STRING,
            category STRING,
            months_active BIGINT,
            total_orders BIGINT,
            avg_monthly_orders DOUBLE,
            stddev_monthly_orders DOUBLE,
            peak_monthly_orders BIGINT,
            low_monthly_orders BIGINT,
            total_revenue DOUBLE,
            avg_monthly_revenue DOUBLE,
            total_buyers BIGINT,
            inventory_turnover_rate DOUBLE,
            recommended_reorder_level BIGINT,
            inventory_category STRING,
            _processed_timestamp TIMESTAMP,
            _analysis_date DATE
        )
        USING DELTA
        PARTITIONED BY (inventory_category)
        LOCATION '{config['mount_point_gold']}/inventory_analytics'
    """)
    
    # Write with merge
    inventory_with_meta.createOrReplaceTempView("new_inventory_data")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.inventory_analytics AS target
        USING new_inventory_data AS source
        ON target.product_id = source.product_id
        
        WHEN MATCHED THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    return inventory_with_meta

# Execute inventory analytics
create_inventory_analytics()