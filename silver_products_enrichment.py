
# ============================================
# MODULE 9: Silver Layer - Products with Category Enrichment
# Description: Enriches product data using broadcast joins
#              with product category translations
# ============================================

def enrich_products_with_broadcast():
    """
    Enrich products with category translations using broadcast join
    Broadcast is efficient for small dimension tables (< 10MB)
    """
    
    # Product category translations (small table - ideal for broadcast)
    category_translations = spark.createDataFrame([
        ("beleza_saude", "Health & Beauty"),
        ("informatica_acessorios", "Computers & Accessories"),
        ("automotivo", "Automotive"),
        ("cama_mesa_banho", "Bed, Bath & Table"),
        ("moveis_decoracao", "Furniture & Decor"),
        ("esporte_lazer", "Sports & Leisure"),
        ("perfumaria", "Perfumery"),
        ("utilidades_domesticas", "Home Utilities"),
        ("telefonia", "Telephony"),
        ("relogios_presentes", "Watches & Gifts"),
        ("alimentos_bebidas", "Food & Beverages"),
        ("bebes", "Baby Products"),
        ("papelaria", "Stationery"),
        ("brinquedos", "Toys"),
        ("pet_shop", "Pet Shop"),
        ("moda_roupas", "Fashion & Clothing"),
        ("calcados", "Shoes"),
        ("eletrodomesticos", "Home Appliances"),
        ("livros", "Books"),
        ("construcao_ferramentas", "Construction & Tools")
    ], ["product_category_name", "product_category_name_english"])
    
    # Broadcast the translations (small table)
    broadcast_translations = broadcast(category_translations)
    
    # Read bronze products
    bronze_products = spark.table(f"{config['database_bronze']}.products_bronze")
    
    # Get max timestamp for incremental load
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_silver']}.products_silver
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    # Enrich products with broadcast join
    enriched_products = (bronze_products
        .filter(col("_ingestion_timestamp") > max_timestamp)
        .join(broadcast_translations, "product_category_name", "left")
        .withColumn("product_name_length", length(col("product_name")))
        .withColumn("product_description_length", length(col("product_description")))
        .withColumn("product_photos_qty", 
            size(split(col("product_photos"), ",")))
        .withColumn("category_level", 
            when(col("product_category_name_english").isNull(), "UNKNOWN")
            .otherwise("KNOWN"))
        .withColumn("_processed_timestamp", current_timestamp())
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
            "category_level",
            "_processed_timestamp"
        )
    )
    
    # Write to silver
    (enriched_products.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"{config['database_silver']}.products_silver")
    )
    
    print(f"Broadcast join complete. Enriched {enriched_products.count()} products")
    
    return enriched_products

# Execute enrichment
enrich_products_with_broadcast()