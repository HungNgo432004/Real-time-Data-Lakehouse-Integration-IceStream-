# CDC Internship Project

Pipeline demo:

`MySQL -> Debezium -> Kafka -> PostgreSQL`

## Project layout

```text
.
|-- config/
|   `-- debezium/connector.json
|-- docs/
|   `-- project-structure.md
|-- scripts/
|   |-- generate_events.py
|   `-- warehouse_loader.py
|-- sql/
|   `-- init/postgres_init.sql
|-- src/
|   `-- cdc_project/
|       |-- settings.py
|       `-- warehouse.py
|-- docker-compose.yml
`-- requirements.txt
```

## Current flow

1. Source data is inserted or updated in MySQL table `inventory.customers`.
2. Debezium captures change events and publishes them to Kafka topic `dbserver1.inventory.customers`.
3. `scripts/warehouse_loader.py` reads CDC messages from Kafka.
4. Data is loaded into PostgreSQL table `dbserver1_inventory_customers`.
5. Processing history is stored in `etl_job_log`.

## Main files

- `docker-compose.yml`: start MySQL, Kafka, Debezium Connect, PostgreSQL
- `config/debezium/connector.json`: Debezium MySQL connector config
- `scripts/generate_events.py`: create sample events in MySQL
- `scripts/warehouse_loader.py`: CDC consumer and warehouse loader
- `sql/init/postgres_init.sql`: initial warehouse schema
- `src/cdc_project`: shared application settings and warehouse logic

## Run project

1. Start containers:

```powershell
docker compose up -d
```

2. Register Debezium connector:

```powershell
Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile "config/debezium/connector.json"
```


3. Install Python dependencies:

```powershell
pip install -r requirements.txt
```

4. Run the warehouse loader:

```powershell
python scripts/warehouse_loader.py
```

5. Generate source events:

```powershell
python scripts/generate_events.py
```

## Scheduler demo with threshold K

There are now 2 scheduler demos:

- `scripts/schedule_demo.py`: simulation only, useful for presentation
- `scripts/scheduled_warehouse_loader.py`: real batch processing from Kafka topic

Rules:

- Input `N` events and threshold `K`
- If `N < K`: create 1 process with `N` events and run `Extract` only
- If `N >= K`: split into batches of size `K`
- Each full batch of `K` events runs full `ETL`
- If there is a remainder batch smaller than `K`, that batch runs `Extract` only

### Simulation demo

Run the demo:

```powershell
python scripts/schedule_demo.py --n 23 --k 10
```

Example interpretation for `N=23`, `K=10`:

- Process 1: 10 events -> `Extract`, `Transform`, `Load`
- Process 2: 10 events -> `Extract`, `Transform`, `Load`
- Process 3: 3 events -> `Extract` only

If you want to slow down the presentation:

```powershell
python scripts/schedule_demo.py --n 23 --k 10 --interval-seconds 5
```

### Real scheduler from Kafka

This job reads the real pending event count `N` from Kafka topic `dbserver1.inventory.customers`.

- Full batch of size `K`: `Extract + Transform + Load`, then commit Kafka offsets
- Remainder batch smaller than `K`: `Extract` only, offsets are not committed so the events remain pending for a future run

Run:

```powershell
python scripts/scheduled_warehouse_loader.py --k 10
```

Useful PostgreSQL tables for checking results:

- `dbserver1_inventory_customers`: loaded warehouse rows
- `etl_job_log`: per-event ETL status
- `extracted_event_log`: raw extracted Kafka events
- `scheduled_process_log`: per-process batch execution log

## Warehouse tables

### `dbserver1_inventory_customers`

Target table for the latest customer state:

- `id`
- `name`
- `email`
- `phone`
- `address`
- `gender`
- `date_of_birth`
- `status`
- `created_at`
- `updated_at`

### `dw_customers`

Compatibility view for reporting queries mapped from `dbserver1_inventory_customers`.

### `etl_job_log`

Simple ETL monitoring table:

- processed operation
- customer id
- processed timestamp
- success, skipped, or failed status
- error message if processing fails

## Demo queries

```sql
SELECT * FROM dbserver1_inventory_customers ORDER BY id DESC LIMIT 10;
SELECT status, COUNT(*) FROM dbserver1_inventory_customers GROUP BY status;
SELECT * FROM etl_job_log ORDER BY processed_at DESC LIMIT 20;
```

## Suggested next development

If you want to extend this internship project further, the next useful steps are:

1. Add a star schema in PostgreSQL with dimension and fact tables.
2. Add a dead-letter topic for failed CDC events.
3. Add dbt or Airflow for batch transformation and scheduling.




docker compose up -d


Invoke-RestMethod -Method Post `
  -Uri http://localhost:8083/connectors `
  -ContentType "application/json" `
  -InFile "config/debezium/connector.json"

pip install -r requirements.txt


python scripts/scheduled_warehouse_loader.py --k 20 --timeout-t 30 --interval-seconds 30

python scripts/generate_events.py --n 27	

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d "{\"name\":\"postgres-sink\",\"config\":{\"connector.class\":\"io.debezium.connector.jdbc.JdbcSinkConnector\",\"tasks.max\":\"1\",\"topics\":\"dbserver1.inventory.customers\",\"connection.url\":\"jdbc:postgresql://postgres:5432/mydb\",\"connection.username\":\"postgres\",\"connection.password\":\"123456\",\"insert.mode\":\"insert\",\"primary.key.mode\":\"none\",\"transforms\":\"unwrap\",\"transforms.unwrap.type\":\"io.debezium.transforms.ExtractNewRecordState\"}}"


docker exec -it kafka bash -c "/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers 

# 1. Khởi động (thêm MinIO mới)
docker compose up -d
# 2. Đăng ký Debezium connector (nếu chưa)
# 3. Chạy pipeline ETL  
python scripts/scheduled_warehouse_loader.py --k 10 --timeout-t 30 --interval-seconds 30
# 4. Mở terminal mới → sinh event
python scripts/generate_events.py --n 27
# 5. Sau ~30s, xem data Iceberg
python scripts/demo_iceberg_query.py
# 6. Demo Schema Evolution (thêm cột mới tự động)
python scripts/demo_schema_evolution.py

docker exec kafka /kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic schema-changes.inventory --from-beginning --max-messages 50

docker exec -it kafka bash -c "/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic dbserver1.inventory.customers 

python scripts/schema_changes_listener.py