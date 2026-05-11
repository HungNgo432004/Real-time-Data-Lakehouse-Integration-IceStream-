"""
Demo script: Query dữ liệu Iceberg bằng DuckDB.

Script này kết nối trực tiếp vào Iceberg table trên MinIO S3,
sử dụng DuckDB để thực hiện:
  1. Xem toàn bộ dữ liệu CDC (history log)
  2. Trích xuất trạng thái mới nhất cho mỗi customer (current state)
  3. Hiển thị schema hiện tại của Iceberg table (chứng minh Schema Evolution)
"""

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cdc_project import settings
from cdc_project.iceberg_warehouse import (
    get_iceberg_catalog,
    get_or_create_iceberg_table,
)


def main():
    print("=" * 70)
    print("  ICEBERG DATA LAKEHOUSE - DEMO QUERY (DuckDB)")
    print("=" * 70)

    # --- 1. Kết nối Iceberg Table ---
    print("\n[1] Ket noi Iceberg Catalog...")
    catalog = get_iceberg_catalog()
    table = get_or_create_iceberg_table(catalog)
    table_id = f"{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_TABLE_NAME}"
    print(f"    Table: {table_id}")

    # --- 2. Hiển thị Schema hiện tại ---
    print("\n[2] Schema hien tai cua Iceberg table:")
    print("-" * 50)
    for field in table.schema().fields:
        print(f"    {field.field_id:>3}  {field.name:<20}  {field.field_type}")
    print("-" * 50)

    # --- 3. Scan toàn bộ dữ liệu (history) ---
    print("\n[3] Toan bo CDC event log (append-only):")
    try:
        scan = table.scan()
        df = scan.to_pandas()

        if df.empty:
            print("    (Chua co du lieu nao trong Iceberg table)")
            return

        print(f"    Tong so records: {len(df)}")
        print()
        print(df.to_string(index=False, max_rows=30))
    except Exception as e:
        print(f"    Loi khi scan: {e}")
        return

    # --- 4. Current State: lấy bản ghi mới nhất cho mỗi id ---
    print("\n" + "=" * 70)
    print("[4] CURRENT STATE (ban ghi moi nhat cho tung customer):")
    print("-" * 50)
    try:
        import duckdb

        con = duckdb.connect()
        # Register pandas DataFrame vào DuckDB
        con.register("cdc_events", df)

        current_state_sql = """
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY id
                        ORDER BY cdc_ts DESC
                    ) AS rn
                FROM cdc_events
                WHERE cdc_op != 'd'
            ) ranked
            WHERE rn = 1
            ORDER BY id
        """

        result = con.execute(current_state_sql).fetchdf()
        # Bỏ cột rn phụ
        if "rn" in result.columns:
            result = result.drop(columns=["rn"])

        print(f"    Tong so customers hien tai: {len(result)}")
        print()
        print(result.to_string(index=False, max_rows=30))

        con.close()
    except ImportError:
        print("    DuckDB chua duoc cai dat. Chay: pip install duckdb")
    except Exception as e:
        print(f"    Loi khi query DuckDB: {e}")

    # --- 5. Thống kê theo operation ---
    print("\n" + "=" * 70)
    print("[5] Thong ke theo CDC operation:")
    print("-" * 50)
    if "cdc_op" in df.columns:
        op_counts = df["cdc_op"].value_counts()
        op_labels = {"c": "CREATE", "u": "UPDATE", "d": "DELETE", "r": "READ/SNAPSHOT"}
        for op, count in op_counts.items():
            label = op_labels.get(op, op)
            print(f"    {label:<20} : {count}")

    print("\n" + "=" * 70)
    print("  DEMO HOAN TAT")
    print("=" * 70)


if __name__ == "__main__":
    main()
