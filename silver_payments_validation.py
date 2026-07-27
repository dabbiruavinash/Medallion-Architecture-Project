
# ============================================
# MODULE 11: Silver Layer - Payments with Validation
# Description: Validates and cleans payment data
#              with business rules and transformations
# ============================================

def validate_and_clean_payments():
    """
    Apply business rules and validations on payment data:
    - Fraud detection flags
    - Payment type standardization
    - Installment validation
    """
    
    # Create custom validation UDF
    @udf(returnType=StringType())
    def validate_payment_type(payment_type, payment_value):
        """Custom validation logic"""
        valid_types = ["credit_card", "debit_card", "voucher", "boleto", "pix"]
        if payment_type not in valid_types:
            return "INVALID"
        if payment_value <= 0:
            return "ZERO_VALUE"
        if payment_value > 50000:  # Suspicious high value
            return "SUSPICIOUS"
        return "VALID"
    
    # Read bronze payments
    bronze_payments = spark.table(f"{config['database_bronze']}.payments_bronze")
    
    # Get incremental data using max timestamp
    try:
        max_timestamp = spark.sql(f"""
            SELECT COALESCE(MAX(_processed_timestamp), '1970-01-01') as max_ts
            FROM {config['database_silver']}.payments_silver
        """).collect()[0]['max_ts']
    except:
        max_timestamp = '1970-01-01'
    
    # Apply transformations and validations
    validated_payments = (bronze_payments
        .filter(col("_ingestion_timestamp") > max_timestamp)
        .withColumn("payment_type_standardized",
            when(col("payment_type") == "credit_card", "CREDIT_CARD")
            .when(col("payment_type") == "debit_card", "DEBIT_CARD")
            .when(col("payment_type") == "boleto", "BOLETO")
            .when(col("payment_type") == "voucher", "VOUCHER")
            .otherwise("OTHER"))
        .withColumn("validation_status", 
            validate_payment_type(col("payment_type"), col("payment_value")))
        .withColumn("is_installment_valid",
            when(col("payment_installments") > 0, true).otherwise(false))
        .withColumn("installment_value",
            round(col("payment_value") / col("payment_installments"), 2))
        .withColumn("is_high_value",
            when(col("payment_value") > 10000, true).otherwise(false))
        .withColumn("payment_method_category",
            when(col("payment_type").isin("credit_card", "debit_card"), "CARD")
            .when(col("payment_type").isin("boleto"), "BANK_SLIP")
            .otherwise("OTHER"))
        .withColumn("_processed_timestamp", current_timestamp())
        .select(
            "order_id",
            "payment_sequential",
            "payment_type_standardized",
            "payment_installments",
            "payment_value",
            "installment_value",
            "validation_status",
            "is_installment_valid",
            "is_high_value",
            "payment_method_category",
            "_processed_timestamp"
        )
    )
    
    # Write validated data
    (validated_payments.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"{config['database_silver']}.payments_silver")
    )
    
    # Log validation statistics
    validation_stats = validated_payments.groupBy("validation_status").count()
    print("Payment Validation Statistics:")
    validation_stats.show()
    
    return validated_payments

# Execute validation
validate_and_clean_payments()