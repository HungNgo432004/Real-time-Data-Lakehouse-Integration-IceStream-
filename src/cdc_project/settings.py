import os


def get_env(name, default):
    return os.getenv(name, default)


KAFKA_TOPIC = get_env("KAFKA_TOPIC", "dbserver1.inventory.customers")
KAFKA_SCHEMA_TOPIC = get_env("KAFKA_SCHEMA_TOPIC", "schema-changes.inventory")
KAFKA_BOOTSTRAP_SERVERS = get_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = get_env("KAFKA_GROUP_ID", "warehouse-loader")
SCHEDULED_KAFKA_GROUP_ID = get_env(
    "SCHEDULED_KAFKA_GROUP_ID", "scheduled-warehouse-loader"
)

POSTGRES_HOST = get_env("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(get_env("POSTGRES_PORT", "5433"))
POSTGRES_DB = get_env("POSTGRES_DB", "mydb")
POSTGRES_USER = get_env("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = get_env("POSTGRES_PASSWORD", "123456")

MYSQL_HOST = get_env("MYSQL_HOST", "localhost")
MYSQL_PORT = int(get_env("MYSQL_PORT", "3306"))
MYSQL_USER = get_env("MYSQL_USER", "root")
MYSQL_PASSWORD = get_env("MYSQL_PASSWORD", "debezium")
MYSQL_DB = get_env("MYSQL_DB", "inventory")

# --- MinIO / S3 ---
S3_ENDPOINT = get_env("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = get_env("S3_ACCESS_KEY", "admin")
S3_SECRET_KEY = get_env("S3_SECRET_KEY", "password")
S3_BUCKET = get_env("S3_BUCKET", "warehouse")

# --- Iceberg Catalog (SQL Catalog backed by PostgreSQL) ---
ICEBERG_CATALOG_NAME = "cdc_catalog"
ICEBERG_NAMESPACE = "cdc_lakehouse"
ICEBERG_TABLE_NAME = "customers_cdc"
