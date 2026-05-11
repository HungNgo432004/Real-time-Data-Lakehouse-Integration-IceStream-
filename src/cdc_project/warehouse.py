import psycopg2
from datetime import date, timedelta
from psycopg2.extras import Json
from psycopg2 import sql as pgsql

from cdc_project import settings

# Cache để tránh chạy ALTER TABLE lặp đi lặp lại cho cùng một cột
_known_pg_columns: set = set()


def _to_date(value):
    """
    Chuyển đổi giá trị date từ Debezium sang Python date.
    Debezium encode kiểu DATE thành số nguyên = số ngày kể từ 1970-01-01.
    Ví dụ: 11097 -> date(2000, 6, 5)
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return date(1970, 1, 1) + timedelta(days=int(value))
    # Nếu đã là string hoặc date object thì giữ nguyên
    return value


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dbserver1_inventory_customers (
    id INTEGER PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    gender TEXT,
    date_of_birth DATE,
    status TEXT,
    created_at TIMESTAMP NULL,
    updated_at TIMESTAMP NULL
);

CREATE TABLE IF NOT EXISTS etl_job_log (
    id BIGSERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    operation CHAR(1) NOT NULL,
    customer_id INTEGER,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS extracted_event_log (
    id BIGSERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_value BIGINT NOT NULL,
    process_run_id TEXT NOT NULL,
    process_id INTEGER NOT NULL,
    operation CHAR(1),
    customer_id INTEGER,
    extract_mode TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (topic_name, partition_id, offset_value)
);

CREATE TABLE IF NOT EXISTS scheduled_process_log (
    id BIGSERIAL PRIMARY KEY,
    process_run_id TEXT NOT NULL,
    process_id INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    mode TEXT NOT NULL,
    planned_event_count INTEGER NOT NULL,
    processed_event_count INTEGER NOT NULL,
    committed_offset_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP VIEW IF EXISTS dw_customers;

CREATE VIEW dw_customers AS
SELECT
    id AS customer_id,
    name AS full_name,
    email,
    phone,
    address,
    gender,
    date_of_birth,
    status,
    created_at AS source_created_at,
    updated_at AS source_updated_at,
    updated_at AS last_event_at
FROM dbserver1_inventory_customers;
"""


def get_postgres_connection():
    return psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )


def ensure_schema(connection):
    with connection.cursor() as cursor:
        cursor.execute(CREATE_TABLE_SQL)
    connection.commit()


def log_etl_event(cursor, topic_name, operation, customer_id, status, error_message=None):
    cursor.execute(
        """
        INSERT INTO etl_job_log (topic_name, operation, customer_id, status, error_message)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (topic_name, operation, customer_id, status, error_message),
    )


def log_extracted_event(
    cursor,
    *,
    process_run_id,
    process_id,
    topic_name,
    partition_id,
    offset_value,
    operation,
    customer_id,
    extract_mode,
    payload,
):
    cursor.execute(
        """
        INSERT INTO extracted_event_log (
            topic_name,
            partition_id,
            offset_value,
            process_run_id,
            process_id,
            operation,
            customer_id,
            extract_mode,
            payload_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (topic_name, partition_id, offset_value) DO UPDATE SET
            process_run_id = EXCLUDED.process_run_id,
            process_id = EXCLUDED.process_id,
            operation = EXCLUDED.operation,
            customer_id = EXCLUDED.customer_id,
            extract_mode = EXCLUDED.extract_mode,
            payload_json = EXCLUDED.payload_json,
            extracted_at = CURRENT_TIMESTAMP
        """,
        (
            topic_name,
            partition_id,
            offset_value,
            process_run_id,
            process_id,
            operation,
            customer_id,
            extract_mode,
            Json(payload),
        ),
    )

def log_scheduled_process(
    cursor,
    *,
    process_run_id,
    process_id,
    topic_name,
    mode,
    planned_event_count,
    processed_event_count,
    committed_offset_count,
    status,
    note=None,
):
    cursor.execute(
        """
        INSERT INTO scheduled_process_log (
            process_run_id,
            process_id,
            topic_name,
            mode,
            planned_event_count,
            processed_event_count,
            committed_offset_count,
            status,
            note
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            process_run_id,
            process_id,
            topic_name,
            mode,
            planned_event_count,
            processed_event_count,
            committed_offset_count,
            status,
            note,
        ),
    )


# Tập các cột đã biết kiểu DATE hoặc TIMESTAMP để convert đúng
_DATE_COLUMNS = {"date_of_birth"}
_TIMESTAMP_COLUMNS = {"created_at", "updated_at"}


def _cast_value(col_name, value):
    """
    Chuyển đổi giá trị từ Debezium sang kiểu Python phù hợp trước khi insert vào PostgreSQL.
    - DATE: số nguyên (epoch days) -> datetime.date
    - Còn lại: giữ nguyên
    """
    if col_name in _DATE_COLUMNS:
        return _to_date(value)
    return value


def ensure_customer_columns(column_names: list, log_fn=None):
    """
    Schema Evolution cho PostgreSQL: tự động thêm cột mới nếu chưa tồn tại.

    - Dùng connection RIÊNG với autocommit=True để DDL commit ngay lập tức,
      đảm bảo column tồn tại trước khi INSERT chạy trong transaction chính.
    - Dùng cache _known_pg_columns để tránh chạy ALTER TABLE không cần thiết.
    - Dùng psycopg2.sql để quote đúng tên cột, tránh lỗi reserved words.
    """
    global _known_pg_columns

    # Lọc ra các cột chưa được cache
    new_cols = [
        col for col in column_names
        if col != "id" and col not in _known_pg_columns
    ]
    if not new_cols:
        return

    # Dùng connection riêng với autocommit để DDL commit ngay
    ddl_conn = psycopg2.connect(
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT,
        database=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
    )
    ddl_conn.autocommit = True
    try:
        with ddl_conn.cursor() as ddl_cursor:
            for col in new_cols:
                ddl_cursor.execute(
                    pgsql.SQL(
                        "ALTER TABLE dbserver1_inventory_customers "
                        "ADD COLUMN IF NOT EXISTS {} TEXT"
                    ).format(pgsql.Identifier(col))
                )
                _known_pg_columns.add(col)
                if log_fn:
                    log_fn(f"[PG SCHEMA EVOLUTION] Da them cot moi vao PostgreSQL: '{col}' (TEXT)")
                else:
                    print(f"[PG SCHEMA EVOLUTION] Da them cot moi vao PostgreSQL: '{col}' (TEXT)", flush=True)
    finally:
        ddl_conn.close()


def upsert_customer(cursor, payload):
    """
    Upsert động: tự phát hiện cột mới từ payload Debezium,
    tự thêm cột vào PostgreSQL nếu cần, rồi build câu SQL INSERT ... ON CONFLICT.
    """
    after = payload.get("after") or {}
    if not after:
        return

    # 1. Đảm bảo tất cả cột trong payload đã tồn tại trong bảng PostgreSQL
    # (dùng connection riêng + autocommit để ALTER TABLE commit ngay lập tức)
    ensure_customer_columns(list(after.keys()))

    # 2. Build dynamic INSERT
    columns = list(after.keys())
    values = [_cast_value(col, after.get(col)) for col in columns]
    placeholders = ", ".join(["%s"] * len(columns))
    # 2b. Build dynamic SQL dùng psycopg2.sql để quote đúng tên cột
    col_identifiers = [pgsql.Identifier(c) for c in columns]
    update_pairs = [
        pgsql.SQL("{} = EXCLUDED.{}").format(pgsql.Identifier(c), pgsql.Identifier(c))
        for c in columns if c != "id"
    ]

    sql = pgsql.SQL(
        "INSERT INTO dbserver1_inventory_customers ({cols}) "
        "VALUES ({placeholders}) "
        "ON CONFLICT (id) DO UPDATE SET {updates}"
    ).format(
        cols=pgsql.SQL(", ").join(col_identifiers),
        placeholders=pgsql.SQL(", ").join(pgsql.Placeholder() for _ in columns),
        updates=pgsql.SQL(", ").join(update_pairs),
    )

    cursor.execute(sql, values)


def delete_customer(cursor, payload):
    before = payload.get("before") or {}
    cursor.execute(
        "DELETE FROM dbserver1_inventory_customers WHERE id = %s",
        (before.get("id"),),
    )

