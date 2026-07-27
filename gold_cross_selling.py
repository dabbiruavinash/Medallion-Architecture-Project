
# ============================================
# MODULE 19: Gold Layer - Cross-Selling Analytics
# Description: Market basket analysis for cross-selling
#              opportunities using co-occurrence
# ============================================

def create_cross_selling_analytics():
    """
    Gold layer: Product affinities and cross-selling opportunities
    Market basket analysis without ML - pure SQL analytics
    """
    
    # Generate product co-occurrence
    cross_selling = spark.sql(f"""
        WITH order_products AS (
            SELECT 
                order_id,
                product_id,
                product_category_name as category
            FROM {config['database_silver']}.order_items_silver oi
            INNER JOIN {config['database_silver']}.products_silver p
                ON oi.product_id = p.product_id
        ),
        product_pairs AS (
            SELECT 
                a.product_id as product_id_a,
                b.product_id as product_id_b,
                a.category as category_a,
                b.category as category_b,
                COUNT(DISTINCT a.order_id) as co_occurrence_count
            FROM order_products a
            INNER JOIN order_products b
                ON a.order_id = b.order_id
                AND a.product_id < b.product_id
            GROUP BY a.product_id, b.product_id, a.category, b.category
            HAVING COUNT(DISTINCT a.order_id) >= 5  -- Minimum threshold
        ),
        product_popularity AS (
            SELECT 
                product_id,
                COUNT(DISTINCT order_id) as total_orders
            FROM order_products
            GROUP BY product_id
        )
        SELECT 
            pp.product_id_a,
            pp.category_a,
            pop_a.total_orders as product_a_orders,
            pp.product_id_b,
            pp.category_b,
            pop_b.total_orders as product_b_orders,
            pp.co_occurrence_count,
            ROUND(pp.co_occurrence_count * 100.0 / NULLIF(pop_a.total_orders, 0), 2) as probability_a_to_b,
            ROUND(pp.co_occurrence_count * 100.0 / NULLIF(pop_b.total_orders, 0), 2) as probability_b_to_a,
            ROUND((pp.co_occurrence_count * 100.0) / (pop_a.total_orders * pop_b.total_orders / 
                (SELECT COUNT(DISTINCT order_id) FROM order_products)), 2) as lift_score
        FROM product_pairs pp
        INNER JOIN product_popularity pop_a ON pp.product_id_a = pop_a.product_id
        INNER JOIN product_popularity pop_b ON pp.product_id_b = pop_b.product_id
        WHERE pp.co_occurrence_count > 10
        ORDER BY lift_score DESC
        LIMIT 1000
    """)
    
    # Add recommendation strength
    cross_selling_with_recs = (cross_selling
        .withColumn("recommendation_strength",
            when(col("lift_score") > 5, "STRONG")
            .when(col("lift_score") > 3, "MODERATE")
            .when(col("lift_score") > 1.5, "WEAK")
            .otherwise("NEGLIGIBLE"))
        .withColumn("cross_category_flag",
            when(col("category_a") != col("category_b"), true).otherwise(false))
        .withColumn("_processed_timestamp", current_timestamp())
        .withColumn("_analysis_date", current_date())
    )
    
    # Create gold table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.cross_selling_opportunities (
            product_id_a STRING,
            category_a STRING,
            product_a_orders BIGINT,
            product_id_b STRING,
            category_b STRING,
            product_b_orders BIGINT,
            co_occurrence_count BIGINT,
            probability_a_to_b DOUBLE,
            probability_b_to_a DOUBLE,
            lift_score DOUBLE,
            recommendation_strength STRING,
            cross_category_flag BOOLEAN,
            _processed_timestamp TIMESTAMP,
            _analysis_date DATE
        )
        USING DELTA
        LOCATION '{config['mount_point_gold']}/cross_selling_opportunities'
    """)
    
    # Full refresh for market basket analysis
    (cross_selling_with_recs.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{config['database_gold']}.cross_selling_opportunities")
    )
    
    return cross_selling_with_recs

# Execute cross-selling analytics
create_cross_selling_analytics()