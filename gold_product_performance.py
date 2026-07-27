
# ============================================
# MODULE 14: Gold Layer - Product Performance Analytics
# Description: Product level KPIs and performance metrics
# ============================================

def create_product_performance():
    """
    Gold layer: Product performance metrics
    Including sales velocity, returns, and category comparisons
    """
    
    # Incremental load using max timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_gold']}.product_performance
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    # Product performance metrics
    product_performance = spark.sql(f"""
        WITH product_sales AS (
            SELECT 
                oi.product_id,
                p.product_category_name_english as category,
                COUNT(DISTINCT oi.order_id) as total_orders,
                COUNT(DISTINCT o.customer_id) as unique_buyers,
                SUM(oi.price) as total_revenue,
                AVG(oi.price) as avg_selling_price,
                SUM(oi.freight_value) / NULLIF(COUNT(*), 0) as avg_freight_cost,
                MIN(o.order_purchase_timestamp) as first_sale_date,
                MAX(o.order_purchase_timestamp) as last_sale_date,
                DATEDIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp)) as product_lifetime_days
            FROM {config['database_silver']}.order_items_silver oi
            INNER JOIN {config['database_silver']}.orders_silver o
                ON oi.order_id = o.order_id
            INNER JOIN {config['database_silver']}.products_silver p
                ON oi.product_id = p.product_id
            WHERE oi._processed_timestamp > '{max_timestamp}'
            GROUP BY oi.product_id, p.product_category_name_english
        ),
        product_returns AS (
            SELECT 
                oi.product_id,
                COUNT(*) as return_count,
                SUM(oi.price) as return_value
            FROM {config['database_silver']}.order_items_silver oi
            INNER JOIN {config['database_silver']}.orders_silver o
                ON oi.order_id = o.order_id
            WHERE o.order_status = 'canceled'
                AND oi._processed_timestamp > '{max_timestamp}'
            GROUP BY oi.product_id
        )
        SELECT 
            ps.product_id,
            ps.category,
            ps.total_orders,
            ps.unique_buyers,
            ps.total_revenue,
            ps.avg_selling_price,
            ps.avg_freight_cost,
            ps.product_lifetime_days,
            COALESCE(pr.return_count, 0) as return_count,
            COALESCE(pr.return_value, 0) as return_value,
            ROUND(COALESCE(pr.return_count, 0) * 100.0 / NULLIF(ps.total_orders, 0), 2) as return_rate,
            ROUND(ps.total_revenue / NULLIF(ps.product_lifetime_days, 0), 2) as daily_revenue_velocity
        FROM product_sales ps
        LEFT JOIN product_returns pr ON ps.product_id = pr.product_id
    """)
    
    # Add performance categories
    product_performance_with_tiers = (product_performance
        .withColumn("revenue_tier",
            when(col("total_revenue") >= 100000, "PREMIUM")
            .when(col("total_revenue") >= 50000, "HIGH_PERFORMER")
            .when(col("total_revenue") >= 10000, "STANDARD")
            .otherwise("LOW_PERFORMER"))
        .withColumn("risk_level",
            when(col("return_rate") > 20, "HIGH_RISK")
            .when(col("return_rate") > 10, "MEDIUM_RISK")
            .otherwise("LOW_RISK"))
        .withColumn("_processed_timestamp", current_timestamp())
    )
    
    # Create and populate gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.product_performance (
            product_id STRING,
            category STRING,
            total_orders BIGINT,
            unique_buyers BIGINT,
            total_revenue DOUBLE,
            avg_selling_price DOUBLE,
            avg_freight_cost DOUBLE,
            product_lifetime_days INT,
            return_count BIGINT,
            return_value DOUBLE,
            return_rate DOUBLE,
            daily_revenue_velocity DOUBLE,
            revenue_tier STRING,
            risk_level STRING,
            _processed_timestamp TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (category)
        LOCATION '{config['mount_point_gold']}/product_performance'
    """)
    
    # Merge with existing data
    product_performance_with_tiers.createOrReplaceTempView("new_product_performance")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.product_performance AS target
        USING new_product_performance AS source
        ON target.product_id = source.product_id
        
        WHEN MATCHED THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    return product_performance_with_tiers

# Execute product performance analytics
create_product_performance()