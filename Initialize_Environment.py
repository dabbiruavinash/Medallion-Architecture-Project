# Databricks Notebook: 00_01_Initialize_Environment.py

# ============================================
# MODULE 1: Environment Setup & Mount Configuration
# Description: Initializes the environment, sets up mounts,
#              creates databases, and configures checkpoints
# ============================================

# Configuration parameters
config = {
    "storage_account": "ecommercedatalake",
    "container_bronze": "bronze",
    "container_silver": "silver", 
    "container_gold": "gold",
    "mount_point_bronze": "/mnt/ecommerce/bronze",
    "mount_point_silver": "/mnt/ecommerce/silver",
    "mount_point_gold": "/mnt/ecommerce/gold",
    "checkpoint_base": "/mnt/ecommerce/checkpoints",
    "database_bronze": "ecom_bronze_db",
    "database_silver": "ecom_silver_db",
    "database_gold": "ecom_gold_db"
}

# Mount Azure Data Lake Storage
def mount_adls(container_name, mount_point):
    """
    Mount Azure Data Lake Storage containers
    Using service principal authentication
    """
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": dbutils.secrets.get("ecom-scope", "client-id"),
        "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("ecom-scope", "client-secret"),
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get('ecom-scope', 'tenant-id')}/oauth2/token"
    }
    
    # Unmount if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)
    
    # Mount the container
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{config['storage_account']}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted {container_name} to {mount_point}")

# Mount all containers
mount_adls(config["container_bronze"], config["mount_point_bronze"])
mount_adls(config["container_silver"], config["mount_point_silver"])
mount_adls(config["container_gold"], config["mount_point_gold"])

# Create databases
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['database_bronze']}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['database_silver']}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {config['database_gold']}")

# Set current databases
spark.sql(f"USE {config['database_bronze']}")