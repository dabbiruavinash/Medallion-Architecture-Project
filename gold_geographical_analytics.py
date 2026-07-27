
# ============================================
# MODULE 17: Gold Layer - Geographical Analytics
# Description: Regional sales patterns and customer distribution
# ============================================

def create_geographical_analytics():
    """
    Gold layer: Geographical distribution and regional performance
    Includes city, state, and zip code level analytics
    """
    
    # Get max process timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_gold']}.geographical_analytics
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    # Geographical analytics with multiple granularities
    geo_analytics = spark.sql(f"""
        WITH customer_locations AS (
            SELECT 
                c.customer_state,
                c.customer_city,
                c.customer_zip_code_prefix,
                COUNT(DISTINCT c.customer_id) as total_customers,
                COUNT(DISTINCT o.order_id) as total_orders,
                SUM(oi.price) as total_revenue,
                AVG(oi.price) as avg_order_value,
                AVG(o.delivery_delay_days) as avg_delay,
                SUM(CASE WHEN o.is_delayed = true THEN 1 ELSE 0 END) as delayed_orders,
                COUNT(DISTINCT oi.seller_id) as active_sellers
            FROM {config['database_silver']}.customers_scd2 c
            INNER JOIN {config['database_silver']}.orders_silver o
                ON c.customer_id = o.customer_id
            INNER JOIN {config['database_silver']}.order_items_silver oi
                ON o.order_id = oi.order_id
            WHERE c.is_current = true
                AND o._processed_timestamp > '{max_timestamp}'
            GROUP BY c.customer_state, c.customer_city, c.customer_zip_code_prefix
        ),
        regional_rankings AS (
            SELECT 
                customer_state,
                customer_city,
                customer_zip_code_prefix,
                total_customers,
                total_orders,
                total_revenue,
                avg_order_value,
                avg_delay,
                delayed_orders,
                active_sellers,
                ROUND(delayed_orders * 100.0 / NULLIF(total_orders, 0), 2) as delay_rate,
                ROUND(total_revenue / NULLIF(total_customers, 0), 2) as revenue_per_customer,
                RANK() OVER (PARTITION BY customer_state ORDER BY total_revenue DESC) as city_rank_in_state
            FROM customer_locations
        )
        SELECT 
            *,
            CASE 
                WHEN revenue_per_customer > 10000 THEN 'PREMIUM_MARKET'
                WHEN revenue_per_customer > 5000 THEN 'HIGH_VALUE_MARKET'
                WHEN revenue_per_customer > 2000 THEN 'STANDARD_MARKET'
                ELSE 'DEVELOPING_MARKET'
            END as market_segment
        FROM regional_rankings
    """)
    
    # Add metadata
    geo_with_meta = (geo_analytics
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_analysis_date", current_date())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.geographical_analytics (
            customer_state STRING,
            customer_city STRING,
            customer_zip_code_prefix INT,
            total_customers BIGINT,
            total_orders BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            avg_delay DOUBLE,
            delayed_orders BIGINT,
            active_sellers BIGINT,
            delay_rate DOUBLE,
            revenue_per_customer DOUBLE,
            city_rank_in_state INT,
            market_segment STRING,
            _processed_timestamp TIMESTAMP,
            _analysis_date DATE
        )
        USING DELTA
        PARTITIONED BY (customer_state, market_segment)
        LOCATION '{config['mount_point_gold']}/geographical_analytics'
    """)
    
    # Write with merge logic
    geo_with_meta.createOrReplaceTempView("new_geo_data")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.geographical_analytics AS target
        USING new_geo_data AS source
        ON target.customer_zip_code_prefix = source.customer_zip_code_prefix
        
        WHEN MATCHED THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    return geo_with_meta

# Execute geographical analytics
create_geographical_analytics()