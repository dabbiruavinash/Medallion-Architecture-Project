
# ============================================
# MODULE 20: Pipeline Orchestration & Monitoring
# Description: Orchestrates all modules with error handling,
#              logging, and monitoring capabilities
# ============================================

from datetime import datetime
import time

class EcommerceMedallionPipeline:
    """
    Main orchestration class for the entire medallion architecture
    """
    
    def __init__(self):
        self.execution_log = []
        self.start_time = datetime.now()
        self.pipeline_status = "INITIATED"
    
    def log_execution(self, module_name, status, records_processed=None, error=None):
        """Log execution details for monitoring"""
        log_entry = {
            "timestamp": datetime.now(),
            "module": module_name,
            "status": status,
            "records_processed": records_processed,
            "error": str(error) if error else None,
            "execution_time_seconds": (datetime.now() - self.start_time).total_seconds()
        }
        self.execution_log.append(log_entry)
        
        # Write to audit table
        spark.createDataFrame([log_entry]).write.mode("append").saveAsTable(
            f"{config['database_gold']}.pipeline_audit_log"
        )
        
        print(f"[{log_entry['timestamp']}] {module_name}: {status} " + 
              (f"- {records_processed} records" if records_processed else ""))
    
    def execute_bronze_layer(self):
        """Execute all bronze layer modules"""
        print("\n" + "="*60)
        print("EXECUTING BRONZE LAYER")
        print("="*60)
        
        bronze_modules = [
            ("orders_ingestion", ingest_orders_to_bronze),
            ("customers_ingestion", ingest_customers_to_bronze),
            ("products_ingestion", ingest_products_to_bronze),
            ("order_items_ingestion", ingest_order_items_to_bronze),
            ("payments_ingestion", ingest_payments_to_bronze)
        ]
        
        for module_name, module_func in bronze_modules:
            try:
                print(f"\nExecuting {module_name}...")
                result = module_func()
                self.log_execution(module_name, "SUCCESS", 
                                 records_processed="Streaming")
            except Exception as e:
                self.log_execution(module_name, "FAILED", error=e)
                raise
    
    def execute_silver_layer(self):
        """Execute all silver layer transformations"""
        print("\n" + "="*60)
        print("EXECUTING SILVER LAYER")
        print("="*60)
        
        silver_modules = [
            ("orders_cdc", process_orders_bronze_to_silver),
            ("customers_scd2", process_customers_scd2),
            ("products_enrichment", enrich_products_with_broadcast),
            ("order_items_aggregation", aggregate_order_items),
            ("payments_validation", validate_and_clean_payments)
        ]
        
        for module_name, module_func in silver_modules:
            try:
                print(f"\nExecuting {module_name}...")
                result = module_func()
                self.log_execution(module_name, "SUCCESS", 
                                 records_processed=result.count() if result else 0)
                time.sleep(2)  # Brief pause between modules
            except Exception as e:
                self.log_execution(module_name, "FAILED", error=e)
                raise
    
    def execute_gold_layer(self):
        """Execute all gold layer aggregations"""
        print("\n" + "="*60)
        print("EXECUTING GOLD LAYER")
        print("="*60)
        
        gold_modules = [
            ("daily_sales", create_daily_sales_gold),
            ("customer_segmentation", create_customer_segmentation),
            ("product_performance", create_product_performance),
            ("seller_analytics", create_seller_analytics),
            ("inventory_analytics", create_inventory_analytics),
            ("geographical_analytics", create_geographical_analytics),
            ("order_lifecycle", create_order_lifecycle_analytics),
            ("cross_selling", create_cross_selling_analytics)
        ]
        
        for module_name, module_func in gold_modules:
            try:
                print(f"\nExecuting {module_name}...")
                result = module_func()
                self.log_execution(module_name, "SUCCESS", 
                                 records_processed=result.count() if result else 0)
                time.sleep(3)  # Brief pause between modules
            except Exception as e:
                self.log_execution(module_name, "FAILED", error=e)
                raise
    
    def generate_execution_report(self):
        """Generate final execution report"""
        print("\n" + "="*60)
        print("PIPELINE EXECUTION REPORT")
        print("="*60)
        
        end_time = datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()
        
        successful_modules = len([log for log in self.execution_log if log['status'] == 'SUCCESS'])
        failed_modules = len([log for log in self.execution_log if log['status'] == 'FAILED'])
        
        print(f"Pipeline Start: {self.start_time}")
        print(f"Pipeline End: {end_time}")
        print(f"Total Duration: {total_duration:.2f} seconds")
        print(f"Total Modules: {successful_modules + failed_modules}")
        print(f"Successful: {successful_modules}")
        print(f"Failed: {failed_modules}")
        
        # Save execution report to gold
        report_df = spark.createDataFrame([{
            "pipeline_run_id": datetime.now().strftime("%Y%m%d%H%M%S"),
            "start_time": self.start_time,
            "end_time": end_time,
            "duration_seconds": total_duration,
            "successful_modules": successful_modules,
            "failed_modules": failed_modules,
            "status": "SUCCESS" if failed_modules == 0 else "PARTIAL_FAILURE"
        }])
        
        report_df.write.mode("append").saveAsTable(
            f"{config['database_gold']}.pipeline_execution_report"
        )
        
        return report_df
    
    def run_pipeline(self):
        """Main pipeline execution method"""
        try:
            print("STARTING ECOMMERCE MEDALLION PIPELINE")
            print(f"Start Time: {self.start_time}")
            
            # Execute layers in sequence
            self.execute_bronze_layer()
            self.execute_silver_layer()
            self.execute_gold_layer()
            
            self.pipeline_status = "COMPLETED"
            
        except Exception as e:
            self.pipeline_status = "FAILED"
            print(f"\nPIPELINE FAILED: {str(e)}")
            self.log_execution("PIPELINE", "FAILED", error=e)
            raise
            
        finally:
            # Always generate report
            self.generate_execution_report()
        
        return self.pipeline_status

# Create audit and reporting tables
def setup_monitoring_tables():
    """Setup monitoring and audit infrastructure"""
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.pipeline_audit_log (
            timestamp TIMESTAMP,
            module STRING,
            status STRING,
            records_processed STRING,
            error STRING,
            execution_time_seconds DOUBLE
        )
        USING DELTA
        LOCATION '{config['mount_point_gold']}/pipeline_audit'
    """)
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config['database_gold']}.pipeline_execution_report (
            pipeline_run_id STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            successful_modules INT,
            failed_modules INT,
            status STRING
        )
        USING DELTA
        LOCATION '{config['mount_point_gold']}/pipeline_reports'
    """)

# Initialize and run
setup_monitoring_tables()
pipeline = EcommerceMedallionPipeline()
pipeline.run_pipeline()