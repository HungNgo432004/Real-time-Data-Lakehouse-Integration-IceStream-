"""
Demo script: Chứng minh Schema Evolution trên Apache Iceberg.

Quy trình demo:
  1. Hiển thị schema ban đầu của Iceberg table.
  2. Thêm cột mới 'loyalty_points' vào MySQL bảng customers.
  3. Insert một record có giá trị loyalty_points trong MySQL.
  4. Chạy ETL pipeline (hoặc chờ pipeline tự chạy).
  5. Iceberg sẽ tự động phát hiện cột mới và thêm vào schema.
  6. Xác nhận schema đã được mở rộng.

LƯU Ý: Script này cần MySQL connector đang hoạt động và pipeline đang chạy.
"""

import sys
import time
from pathlib import Path

import mysql.connector

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cdc_project import settings
from cdc_project.iceberg_warehouse import (
    get_iceberg_catalog,
    get_or_create_iceberg_table,
)


def print_iceberg_schema(table, label=""):
    """In schema hiện tại của Iceberg table."""
    print(f"\n{'='*60}")
    print(f"  ICEBERG SCHEMA {label}")
    print(f"{'='*60}")
    for field in table.schema().fields:
        print(f"  {field.field_id:>3}  {field.name:<25} {field.field_type}")
    print(f"{'='*60}\n")


def main():
    print("=" * 60)
    print("  DEMO: SCHEMA EVOLUTION VOI APACHE ICEBERG")
    print("=" * 60)

    # 1. Kết nối Iceberg
    print("\n[Buoc 1] Ket noi Iceberg Catalog...")
    catalog = get_iceberg_catalog()
    table = get_or_create_iceberg_table(catalog)
    print_iceberg_schema(table, "(TRUOC KHI THAY DOI)")

    # Lấy danh sách tên cột hiện tại
    current_columns = [f.name for f in table.schema().fields]
    print(f"  So cot hien tai: {len(current_columns)}")
    print(f"  Cac cot: {current_columns}\n")

    # 2. Thêm cột mới vào MySQL
    print("[Buoc 2] Them cot 'loyalty_points' vao bang customers trong MySQL...")
    conn = mysql.connector.connect(
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DB,
    )
    cursor = conn.cursor()

    try:
        cursor.execute(
            "ALTER TABLE customers ADD COLUMN loyalty_points INT DEFAULT 0"
        )
        conn.commit()
        print("  -> Da them cot 'loyalty_points' vao MySQL thanh cong!\n")
    except mysql.connector.errors.ProgrammingError as e:
        if "Duplicate column name" in str(e):
            print("  -> Cot 'loyalty_points' da ton tai trong MySQL. Tiep tuc...\n")
        else:
            raise

    # 3. Insert record mới có loyalty_points
    print("[Buoc 3] Insert 1 customer moi co 'loyalty_points'...")
    batch_suffix = int(time.time())
    cursor.execute(
        "INSERT INTO customers (name, email, phone, loyalty_points) "
        "VALUES (%s, %s, %s, %s)",
        (
            "SchemaEvo_Demo_User",
            f"schema_evo_{batch_suffix}@test.com",
            "0999999999",
            150,
        ),
    )
    conn.commit()
    print(f"  -> Da insert customer 'SchemaEvo_Demo_User' voi loyalty_points=150\n")

    cursor.close()
    conn.close()

    # 4. Hướng dẫn chờ pipeline
    print("[Buoc 4] Hay cho pipeline ETL xu ly event nay...")
    print("  Pipeline se:")
    print("    a) Doc event tu Kafka (chua gia tri 'loyalty_points')")
    print("    b) Phat hien cot 'loyalty_points' chua co trong Iceberg schema")
    print("    c) Tu dong goi update_schema().add_column('loyalty_points')")
    print("    d) Append data thanh cong voi cau truc moi")
    print()
    print("  Neu pipeline dang chay voi --interval-seconds 30, doi khoang 30-60 giay...")
    print()

    input("  >>> Nhan ENTER sau khi pipeline da xu ly event de xem ket qua... ")

    # 5. Kiểm tra lại schema
    print("\n[Buoc 5] Kiem tra lai Iceberg schema SAU khi pipeline xu ly...")
    # Reload table để lấy schema mới nhất
    table_id = f"{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_TABLE_NAME}"
    table = catalog.load_table(table_id)
    print_iceberg_schema(table, "(SAU KHI SCHEMA EVOLUTION)")

    new_columns = [f.name for f in table.schema().fields]
    added = set(new_columns) - set(current_columns)

    if added:
        print(f"  THANH CONG! Cac cot moi da duoc tu dong them: {added}")
    else:
        print("  Chua phat hien cot moi. Co the pipeline chua xu ly xong.")
        print("  Thu chay lai script sau khi pipeline da xu ly event.")

    print("\n" + "=" * 60)
    print("  DEMO SCHEMA EVOLUTION HOAN TAT")
    print("=" * 60)


if __name__ == "__main__":
    main()
