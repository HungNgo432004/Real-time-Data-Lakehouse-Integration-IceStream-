import sys
import time
from pathlib import Path

import mysql.connector

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cdc_project import settings


def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description="Tao du lieu mau.")
    parser.add_argument("--n", type=int, default=10, help="So luong events tao ra")
    return parser.parse_args()


def main():
    args = parse_args()
    conn = mysql.connector.connect(
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DB,
    )
    cursor = conn.cursor()

    print(f"Start generating {args.n} events...")
    batch_suffix = int(time.time())

    for i in range(args.n):
        name = f"User_{i}"
        email = f"user{i}_{batch_suffix}@gmail.com"
        phone = f"09{i:08d}"

        cursor.execute(
            "INSERT INTO customers (name, email, phone) VALUES (%s, %s, %s)",
            (name, email, phone),
        )

        if i % 100 == 0:
            conn.commit()
            print(f"Inserted {i} records")

    conn.commit()
    conn.close()
    print("DONE generating events")


if __name__ == "__main__":
    main()
