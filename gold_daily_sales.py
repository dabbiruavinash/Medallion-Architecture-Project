
# ============================================
# MODULE 12: Gold Layer - Daily Sales Aggregation
# Description: Aggregates daily sales metrics using 
#              max(timestamp) for incremental processing
# ============================================

def create_daily_sales_gold():
    """
    Gold layer: Daily sales aggregation
    Uses max(timestamp) from gold table for incremental loads
    """
    
    # Get max processed date for incremental load
    try:
        max_date = spark.sql(f"""
            SELECT CAST(COALESCE(MAX(report_date), '1970-01-01') AS STRING) as max_date
            FROM {config['database_gold']}.daily_sales_summary
        """).collect()[0]['max_date']
    except:
        max_date = '1970-01-01'
    
    print(f"Incrementally processing sales after date: {max_date}")
    
    # Create daily sales aggregation
    daily_sales = spark.sql(f"""
        SELECT 
            DATE(o.order_purchase_timestamp) as report_date,
            COUNT(DISTINCT o.order_id) as total_orders,
            COUNT(DISTINCT o.customer_id) as unique_customers,
            SUM(oi.price) as total_revenue,
            AVG(oi.price) as avg_order_value,
            SUM(CASE WHEN o.is_delayed = true THEN 1 ELSE 0 END) as delayed_orders,
            ROUND(SUM(CASE WHEN o.is_delayed = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_rate,
            SUM(p.payment_value) as total_payments,
            COUNT(DISTINCT p.payment_type_standardized) as payment_methods_used,
            ROUND(AVG(o.processing_time_days), 1) as avg_processing_time_days,
            SUM(oi.freight_value) as total_freight,
            ROUND(SUM(oi.freight_value) * 100.0 / NULLIF(SUM(oi.price), 0), 2) as freight_percentage
        FROM {config['database_silver']}.orders_silver o
        INNER JOIN {config['database_silver']}.order_items_silver oi 
            ON o.order_id = oi.order_id
        INNER JOIN {config['database_silver']}.payments_silver p 
            ON o.order_id = p.order_id
        WHERE DATE(o.order_purchase_timestamp) > '{max_date}'
        GROUP BY DATE(o.order_purchase_timestamp)
    """)
    
    # Add metadata columns
    daily_sales_with_meta = (daily_sales
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_load_type", lit("INCREMENTAL"))
        .withColumn("_report_year_month", date_format("report_date", "yyyy-MM"))
    )
    
    # Create gold table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.daily_sales_summary (
            report_date DATE,
            total_orders BIGINT,
            unique_customers BIGINT,
            total_revenue DOUBLE,
            avg_order_value DOUBLE,
            delayed_orders BIGINT,
            delay_rate DOUBLE,
            total_payments DOUBLE,
            payment_methods_used BIGINT,
            avg_processing_time_days DOUBLE,
            total_freight DOUBLE,
            freight_percentage DOUBLE,
            _processed_timestamp TIMESTAMP,
            _load_type STRING,
            _report_year_month STRING
        )
        USING DELTA
        PARTITIONED BY (_report_year_month)
        LOCATION '{config['mount_point_gold']}/daily_sales_summary'
    """)
    
    # Merge with gold table
    daily_sales_with_meta.createOrReplaceTempView("new_daily_sales")
    
    merge_query = f"""
        MERGE INTO {config['database_gold']}.daily_sales_summary AS target
        USING new_daily_sales AS source
        ON target.report_date = source.report_date
        
        WHEN MATCHED THEN
            UPDATE SET *
            
        WHEN NOT MATCHED THEN
            INSERT *
    """
    
    spark.sql(merge_query)
    
    print(f"Daily sales gold table updated. Records processed: {daily_sales_with_meta.count()}")
    
    return daily_sales_with_meta

# Execute daily sales aggregation
create_daily_sales_gold()