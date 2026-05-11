"""
Iceberg Warehouse module.

Quản lý Apache Iceberg table cho CDC data:
- Tạo và kết nối SQL Catalog (backed by PostgreSQL)
- Tự động phát hiện và thêm cột mới (Schema Evolution)
- Append batch CDC events dưới dạng PyArrow Table vào Iceberg
"""

from datetime import datetime

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchTableError,
)
import time
from pyiceberg.schema import Schema
from pyiceberg.types import (
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from cdc_project import settings


# ---------------------------------------------------------------------------
# Catalog helpers
# ---------------------------------------------------------------------------

def get_iceberg_catalog():
    """Tạo và trả về SQL Catalog kết nối PostgreSQL + MinIO S3."""
    catalog = SqlCatalog(
        settings.ICEBERG_CATALOG_NAME,
        **{
            "uri": (
                f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}"
                f"@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
            ),
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            # s3fs / FsspecFileIO config keys
            "s3.endpoint": settings.S3_ENDPOINT,
            "s3.access-key-id": settings.S3_ACCESS_KEY,
            "s3.secret-access-key": settings.S3_SECRET_KEY,
            "s3.region": "us-east-1",
            # PyIceberg FsspecFileIO truyền các key dưới dạng s3fs storage_options
            "s3.connect-timeout": "60",
            "warehouse": f"s3://{settings.S3_BUCKET}",
        },
    )
    return catalog


def _get_s3fs():
    """Tạo s3fs.S3FileSystem với endpoint MinIO cục bộ (dùng cho scan/read)."""
    import s3fs
    return s3fs.S3FileSystem(
        key=settings.S3_ACCESS_KEY,
        secret=settings.S3_SECRET_KEY,
        endpoint_url=settings.S3_ENDPOINT,
        client_kwargs={"endpoint_url": settings.S3_ENDPOINT},
    )


def ensure_iceberg_namespace(catalog):
    """Tạo namespace nếu chưa tồn tại."""
    try:
        catalog.create_namespace(settings.ICEBERG_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass


# ---------------------------------------------------------------------------
# Schema definition – baseline columns từ bảng customers MySQL
# ---------------------------------------------------------------------------

# Schema khởi tạo phản ánh cấu trúc ban đầu của bảng MySQL customers.
# Khi Schema Evolution xảy ra, các cột mới sẽ được tự động bổ sung.
BASELINE_SCHEMA = Schema(
    NestedField(field_id=1, name="cdc_op", field_type=StringType(), required=False),
    NestedField(field_id=2, name="cdc_ts", field_type=TimestampType(), required=False),
    NestedField(field_id=3, name="kafka_topic", field_type=StringType(), required=False),
    NestedField(field_id=4, name="kafka_partition", field_type=IntegerType(), required=False),
    NestedField(field_id=5, name="kafka_offset", field_type=LongType(), required=False),
    NestedField(field_id=6, name="id", field_type=IntegerType(), required=False),
    NestedField(field_id=7, name="name", field_type=StringType(), required=False),
    NestedField(field_id=8, name="email", field_type=StringType(), required=False),
    NestedField(field_id=9, name="phone", field_type=StringType(), required=False),
    NestedField(field_id=10, name="address", field_type=StringType(), required=False),
    NestedField(field_id=11, name="gender", field_type=StringType(), required=False),
    NestedField(field_id=12, name="date_of_birth", field_type=StringType(), required=False),
    NestedField(field_id=13, name="status", field_type=StringType(), required=False),
    NestedField(field_id=14, name="created_at", field_type=StringType(), required=False),
    NestedField(field_id=15, name="updated_at", field_type=StringType(), required=False),
)


def get_or_create_iceberg_table(catalog):
    """Lấy hoặc tạo mới Iceberg table với BASELINE_SCHEMA."""
    table_identifier = f"{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_TABLE_NAME}"
    try:
        table = catalog.load_table(table_identifier)
    except NoSuchTableError:
        table = catalog.create_table(table_identifier, schema=BASELINE_SCHEMA)
    return table


# ---------------------------------------------------------------------------
# Schema Evolution
# ---------------------------------------------------------------------------

def _iceberg_column_names(table):
    """Trả về tập tên cột hiện tại của Iceberg table."""
    return {field.name for field in table.schema().fields}


def evolve_schema_if_needed(table, new_columns: dict, log_fn=None):
    """
    So sánh cột mới trong payload CDC với schema hiện tại.
    Nếu có cột chưa tồn tại -> tự động thêm vào Iceberg table.

    Args:
        table: pyiceberg Table object
        new_columns: dict các cột mới {column_name: sample_value}
        log_fn: hàm ghi log (optional)
    
    Returns:
        list tên các cột đã được thêm mới
    """
    existing = _iceberg_column_names(table)
    added_columns = []

    # Tìm field_id lớn nhất hiện tại để gán id tiếp theo
    max_field_id = max(f.field_id for f in table.schema().fields)

    columns_to_add = []
    for col_name in sorted(new_columns.keys()):
        if col_name not in existing:
            max_field_id += 1
            columns_to_add.append((col_name, max_field_id))
            added_columns.append(col_name)

    if columns_to_add:
        with table.update_schema() as update:
            for col_name, _ in columns_to_add:
                # Tất cả cột mới đều thêm dưới dạng optional StringType
                # để đảm bảo backward compatibility
                update.add_column(col_name, StringType())
                if log_fn:
                    log_fn(f"[SCHEMA EVOLUTION] Da them cot moi: '{col_name}' (StringType)")

    return added_columns


# ---------------------------------------------------------------------------
# Batch CDC → PyArrow → Iceberg append
# ---------------------------------------------------------------------------

def _extract_record_from_message(message):
    """
    Chuyển đổi một Kafka message (Debezium CDC) thành một flat dict
    để đưa vào PyArrow Table.
    """
    payload = message.value or {}
    operation = payload.get("op")
    after = payload.get("after") or {}
    before = payload.get("before") or {}

    # Lấy dữ liệu từ after (cho c/u/r) hoặc before (cho d)
    data = after if operation in ("c", "u", "r") else before

    record = {
        "cdc_op": operation,
        "cdc_ts": datetime.now(),
        "kafka_topic": message.topic,
        "kafka_partition": message.partition,
        "kafka_offset": message.offset,
    }

    # Đưa toàn bộ field từ data vào record
    for key, value in data.items():
        # Convert mọi giá trị sang string (trừ id giữ int)
        if key == "id":
            record[key] = value
        else:
            record[key] = str(value) if value is not None else None

    return record


def _build_arrow_table(records, iceberg_table):
    """
    Xây dựng PyArrow table từ list records, khớp với schema Iceberg hiện tại.
    """
    if not records:
        return None

    schema = iceberg_table.schema()

    # Tạo mapping: tên cột -> PyArrow type
    pa_fields = []
    for field in schema.fields:
        if field.field_type == IntegerType():
            pa_fields.append(pa.field(field.name, pa.int32()))
        elif field.field_type == LongType():
            pa_fields.append(pa.field(field.name, pa.int64()))
        elif field.field_type == TimestampType():
            pa_fields.append(pa.field(field.name, pa.timestamp("us")))
        else:
            pa_fields.append(pa.field(field.name, pa.string()))

    pa_schema = pa.schema(pa_fields)

    # Tạo columns dict
    columns = {}
    for pa_field in pa_schema:
        col_values = []
        for rec in records:
            col_values.append(rec.get(pa_field.name))
        columns[pa_field.name] = col_values

    return pa.table(columns, schema=pa_schema)


def append_batch_to_iceberg(iceberg_table, kafka_messages, log_fn=None):
    """
    Pipeline chính: 
    1. Extract records từ Kafka messages
    2. Phát hiện cột mới -> Schema Evolution
    3. Build PyArrow Table
    4. Append vào Iceberg

    Args:
        iceberg_table: pyiceberg Table object
        kafka_messages: list các Kafka ConsumerRecord
        log_fn: hàm ghi log

    Returns:
        (int, list): (số record đã append, list cột mới đã thêm)
    """
    records = [_extract_record_from_message(msg) for msg in kafka_messages]

    # Xác định tất cả các cột xuất hiện trong batch này
    all_keys = {}
    for rec in records:
        for k, v in rec.items():
            if k not in all_keys:
                all_keys[k] = v

    # Schema Evolution: kiểm tra và thêm cột nếu cần
    added_columns = evolve_schema_if_needed(iceberg_table, all_keys, log_fn=log_fn)

    if added_columns:
        # Reload table sau khi schema đã thay đổi
        iceberg_table = iceberg_table.refresh()

    # Build Arrow table và append với cơ chế Retry nếu bị tranh chấp commit
    arrow_table = _build_arrow_table(records, iceberg_table)

    if arrow_table is not None and len(arrow_table) > 0:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                iceberg_table.append(arrow_table)
                if log_fn:
                    log_fn(f"[ICEBERG] Da append {len(arrow_table)} records vao Iceberg table.")
                break
            except CommitFailedException as e:
                if attempt < max_retries - 1:
                    if log_fn:
                        log_fn(f"[ICEBERG RETRY] Xung dot commit (lan {attempt+1}), dang refresh table va thu lai...")
                    time.sleep(1)
                    iceberg_table = iceberg_table.refresh()
                else:
                    if log_fn:
                        log_fn(f"[ICEBERG ERROR] That bai sau {max_retries} lan thu vi xung dot commit: {e}")
                    raise
    
    return len(records), added_columns
