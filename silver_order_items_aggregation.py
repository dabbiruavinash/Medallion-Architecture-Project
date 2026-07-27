# ============================================
# MODULE 10: Silver Layer - Order Items with Window Functions
# Description: Aggregates order items using window functions
#              for running totals and rankings
# ============================================

from pyspark.sql.window import Window

def aggregate_order_items():
    """
    Process order items with advanced window functions:
    - Running totals per order
    - Item rankings
    - Revenue contribution
    """
    
    # Read bronze order items with watermark logic
    bronze_items = spark.table(f"{config['database_bronze']}.order_items_bronze")
    
    # Get max processing timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_silver']}.order_items_silver
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    # Define window specifications
    order_window = Window.partitionBy("order_id").orderBy("price")
    seller_window = Window.partitionBy("seller_id")
    
    # Apply window functions
    processed_items = (bronze_items
        .filter(col("_ingestion_timestamp") > max_timestamp)
        .withColumn("item_rank", row_number().over(order_window))
        .withColumn("running_total", sum("price").over(order_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
        .withColumn("order_total", sum("price").over(Window.partitionBy("order_id")))
        .withColumn("revenue_contribution_pct", 
            round((col("price") / col("order_total")) * 100, 2))
        .withColumn("avg_seller_price", avg("price").over(seller_window))
        .withColumn("seller_price_variance", 
            col("price") - col("avg_seller_price"))
        .withColumn("is_above_avg", 
            when(col("seller_price_variance") > 0, true).otherwise(false))
        .withColumn("freight_ratio", 
            round(col("freight_value") / col("price"), 2))
        .withColumn("_processed_timestamp", current_timestamp())
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
            "item_rank",
            "running_total",
            "order_total",
            "revenue_contribution_pct",
            "avg_seller_price",
            "seller_price_variance",
            "is_above_avg",
            "freight_ratio",
            "_processed_timestamp"
        )
    )
    
    # Write to silver
    (processed_items.write
        .format("delta")
        .mode("append")
        .partitionBy("seller_id")
        .saveAsTable(f"{config['database_silver']}.order_items_silver")
    )
    
    return processed_items

# Execute aggregation
aggregate_order_items()