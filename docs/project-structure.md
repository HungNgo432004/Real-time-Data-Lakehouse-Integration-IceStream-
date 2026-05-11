# Project Structure

## Directories

- `config/debezium`: connector configuration for Debezium
- `docs`: project notes and structure documentation
- `scripts`: runnable entrypoints for demo and ETL
- `sql/init`: PostgreSQL initialization scripts
- `src/cdc_project`: reusable Python modules for settings and warehouse logic

## Main flow

1. `scripts/generate_events.py` inserts sample data into MySQL.
2. Debezium publishes CDC messages to Kafka.
3. `scripts/warehouse_loader.py` consumes Kafka and writes into PostgreSQL.
