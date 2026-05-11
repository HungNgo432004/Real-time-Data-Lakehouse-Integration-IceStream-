import sys
import json
import re
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from kafka import KafkaConsumer
from pyiceberg.types import StringType, IntegerType, TimestampType
from cdc_project import settings
from cdc_project.warehouse import get_postgres_connection, _known_pg_columns
from cdc_project.iceberg_warehouse import get_iceberg_catalog, get_or_create_iceberg_table

def process_schema_change(ddl, pg_conn, catalog):
    if not ddl:
        return
    
    # Biểu thức chính quy (Regex) tìm lệnh RENAME
    rename_pattern = re.compile(r"(?i)ALTER\s+TABLE\s+(?:`?inventory`?\.`?customers`?|`?customers`?)\s+RENAME\s+COLUMN\s+`?(\w+)`?\s+TO\s+`?(\w+)`?")
    rename_match = rename_pattern.search(ddl)
    
    # Biểu thức chính quy (Regex) tìm lệnh DROP
    drop_pattern = re.compile(r"(?i)ALTER\s+TABLE\s+(?:`?inventory`?\.`?customers`?|`?customers`?)\s+DROP\s+(?:COLUMN\s+)?`?(\w+)`?")
    drop_match = drop_pattern.search(ddl)

    # Biểu thức chính quy (Regex) tìm lệnh ADD COLUMN
    # Ho tro: ADD COLUMN `col` TYPE, ADD `col` TYPE
    add_pattern = re.compile(r"(?i)ALTER\s+TABLE\s+(?:`?inventory`?\.`?customers`?|`?customers`?)\s+ADD\s+(?:COLUMN\s+)?`?(\w+)`?\s+([\w\(\)]+)", re.DOTALL)
    add_match = add_pattern.search(ddl)

    # Biểu thức chính quy (Regex) tìm lệnh MODIFY/CHANGE COLUMN (MySQL đặc thù)
    # MODIFY `col` TYPE
    modify_pattern = re.compile(r"(?i)ALTER\s+TABLE\s+(?:`?inventory`?\.`?customers`?|`?customers`?)\s+MODIFY\s+(?:COLUMN\s+)?`?(\w+)`?\s+([\w\(\)]+)", re.DOTALL)
    modify_match = modify_pattern.search(ddl)
    # CHANGE `old` `new` TYPE
    change_pattern = re.compile(r"(?i)ALTER\s+TABLE\s+(?:`?inventory`?\.`?customers`?|`?customers`?)\s+CHANGE\s+(?:COLUMN\s+)?`?(\w+)`?\s+`?(\w+)`?\s+([\w\(\)]+)", re.DOTALL)
    change_match = change_pattern.search(ddl)
    
    if rename_match:
        old_col = rename_match.group(1)
        new_col = rename_match.group(2)
        print(f"[SCHEMA CHANGE] Phat hien yeu cau RENAME COLUMN tu '{old_col}' sang '{new_col}'")
        
        # 1. Update Postgres
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(f'ALTER TABLE dbserver1_inventory_customers RENAME COLUMN "{old_col}" TO "{new_col}"')
            pg_conn.commit()
            print(f"  -> [PostgreSQL] Da rename column '{old_col}' thanh '{new_col}'")
        except Exception as e:
            pg_conn.rollback()
            print(f"  -> [PostgreSQL Info] Bo qua rename: {e}")

        # 2. Update Iceberg
        try:
            iceberg_table = get_or_create_iceberg_table(catalog)
            # Kiem tra field
            if any(f.name == old_col for f in iceberg_table.schema().fields):
                with iceberg_table.update_schema() as update:
                    update.rename_column(old_col, new_col)
                print(f"  -> [Iceberg] Da rename column '{old_col}' thanh '{new_col}'")
        except Exception as e:
            print(f"  -> [Iceberg Info] Bo qua rename: {e}")

    elif drop_match:
        dropped_col = drop_match.group(1)
        print(f"[SCHEMA CHANGE] Phat hien yeu cau DROP COLUMN: '{dropped_col}'")
        
        # 1. Update Postgres
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(f'ALTER TABLE dbserver1_inventory_customers DROP COLUMN IF EXISTS "{dropped_col}"')
            pg_conn.commit()
            if dropped_col in _known_pg_columns:
                _known_pg_columns.remove(dropped_col)
            print(f"  -> [PostgreSQL] Da drop column '{dropped_col}'")
        except Exception as e:
            pg_conn.rollback()
            print(f"  -> [PostgreSQL Info] Bo qua drop: {e}")
            
        # 2. Update Iceberg
        try:
            iceberg_table = get_or_create_iceberg_table(catalog)
            if any(f.name == dropped_col for f in iceberg_table.schema().fields):
                with iceberg_table.update_schema() as update:
                    update.delete_column(dropped_col)
                print(f"  -> [Iceberg] Da drop column '{dropped_col}'")
            else:
                print(f"  -> [Iceberg] Cot '{dropped_col}' khong ton tai trong Iceberg.")
        except Exception as e:
            print(f"  -> [Iceberg Info] Bo qua drop: {e}")

    elif add_match:
        new_col = add_match.group(1)
        col_type = add_match.group(2).upper()
        print(f"[SCHEMA CHANGE] Phat hien yeu cau ADD COLUMN: '{new_col}' ({col_type})")

        # 1. Update Postgres
        pg_type = "TEXT"
        if "INT" in col_type: pg_type = "INTEGER"
        elif "TIMESTAMP" in col_type or "DATETIME" in col_type: pg_type = "TIMESTAMP"
        
        try:
            with pg_conn.cursor() as cursor:
                cursor.execute(f'ALTER TABLE dbserver1_inventory_customers ADD COLUMN IF NOT EXISTS "{new_col}" {pg_type}')
            pg_conn.commit()
            print(f"  -> [PostgreSQL] Da add column '{new_col}' ({pg_type})")
        except Exception as e:
            pg_conn.rollback()
            print(f"  -> [PostgreSQL Info] Bo qua add: {e}")

        # 2. Update Iceberg
        try:
            iceberg_table = get_or_create_iceberg_table(catalog)
            if not any(f.name == new_col for f in iceberg_table.schema().fields):
                # Mapping kieu Iceberg
                iceberg_type = StringType()
                if "INT" in col_type: iceberg_type = IntegerType()
                elif "TIMESTAMP" in col_type or "DATETIME" in col_type: iceberg_type = TimestampType()
                
                with iceberg_table.update_schema() as update:
                    update.add_column(new_col, iceberg_type)
                print(f"  -> [Iceberg] Da add column '{new_col}' ({iceberg_type})")
        except Exception as e:
            print(f"  -> [Iceberg Info] Bo qua add: {e}")

    elif modify_match or change_match:
        if modify_match:
            col_name = modify_match.group(1)
            new_type = modify_match.group(2).upper()
            print(f"[SCHEMA CHANGE] Phat hien yeu cau MODIFY COLUMN: '{col_name}' sang kieu '{new_type}'")
        else:
            old_col = change_match.group(1)
            new_col = change_match.group(2)
            new_type = change_match.group(3).upper()
            print(f"[SCHEMA CHANGE] Phat hien yeu cau CHANGE COLUMN: '{old_col}' sang '{new_col}' ({new_type})")
            # Neu rename trong sequence thi change_match se bao gom ca rename
            # O day chung ta log thong tin truoc
        
        print("  -> [Info] Hien tai chi ho tro log va tu dong evolution khi co data event.")
        print("  -> [Notice] De update schema ngay lap tuc cho type change, can logic mapping phuc tap hon.")

def main():
    print(f"Khoi dong Schema Changes Listener. Dang lang nghe topic: {settings.KAFKA_SCHEMA_TOPIC}")
    
    catalog = get_iceberg_catalog()
    pg_conn = get_postgres_connection()
    
    consumer = KafkaConsumer(
        settings.KAFKA_SCHEMA_TOPIC,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"schema-changes-listener-group-{int(time.time())}", # Use unique group ID for verification
        auto_offset_reset="earliest", # Quét lại để bắt sự kiện xóa gần đây
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")) if raw is not None else None,
    )
    
    try:
        for message in consumer:
            payload = message.value
            if not payload:
                continue
            
            ddl = payload.get("ddl")
            if ddl:
                process_schema_change(ddl, pg_conn, catalog)
            else:
                # Heartbeat logging for messages without DDL
                pos = payload.get("position", {})
                print(f"[DEBUG] Received message from {pos.get('file')}:{pos.get('pos')}, but no DDL found.")
                
    except KeyboardInterrupt:
        print("\nListener da bi ngat.")
    finally:
        consumer.close()
        pg_conn.close()

if __name__ == "__main__":
    main()
