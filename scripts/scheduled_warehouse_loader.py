import argparse
import json
import sys
import time
from contextlib import closing
from datetime import datetime
from pathlib import Path

import schedule

from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from cdc_project import settings
from cdc_project.warehouse import (
    delete_customer,
    ensure_schema,
    get_postgres_connection,
    log_etl_event,
    log_extracted_event,
    log_scheduled_process,
    upsert_customer,
)
from cdc_project.iceberg_warehouse import (
    append_batch_to_iceberg,
    ensure_iceberg_namespace,
    get_iceberg_catalog,
    get_or_create_iceberg_table,
)


def log(message):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Xu ly batch ETL that tu Kafka theo nguong K."
    )
    parser.add_argument("--k", type=int, required=True, help="Nguong batch K")
    parser.add_argument(
        "--group-id",
        default=settings.SCHEDULED_KAFKA_GROUP_ID,
        help="Consumer group rieng cho scheduled ETL",
    )
    parser.add_argument(
        "--poll-timeout-ms",
        type=int,
        default=5000,
        help="Thoi gian doi cho moi lan poll Kafka",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Chu ky loop script (giay)",
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=None,
        help="Chu ky loop script (phut)",
    )
    parser.add_argument(
        "--interval-hours",
        type=int,
        default=None,
        help="Chu ky loop script (gio)",
    )
    parser.add_argument(
        "--timeout-t",
        type=int,
        default=60,
        help="Thoi gian T (giay) de ep buoc thuc hien ETL cho cac event dang cho",
    )
    return parser.parse_args()


def build_consumer(group_id):
    return KafkaConsumer(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")) if raw is not None else None,
    )


def get_topic_partitions(consumer):
    partitions = consumer.partitions_for_topic(settings.KAFKA_TOPIC)
    if not partitions:
        return []
    topic_partitions = [
        TopicPartition(settings.KAFKA_TOPIC, partition_id)
        for partition_id in sorted(partitions)
    ]
    consumer.assign(topic_partitions)
    return topic_partitions


def get_pending_offsets(consumer, topic_partitions):
    beginning_offsets = consumer.beginning_offsets(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)

    offset_plan = {}
    total_pending = 0

    for topic_partition in topic_partitions:
        committed_offset = consumer.committed(topic_partition)
        start_offset = (
            committed_offset
            if committed_offset is not None
            else beginning_offsets[topic_partition]
        )
        end_offset = end_offsets[topic_partition]
        pending = max(0, end_offset - start_offset)
        offset_plan[topic_partition] = {
            "start_offset": start_offset,
            "end_offset": end_offset,
            "pending": pending,
        }
        total_pending += pending

    return total_pending, offset_plan


def seek_to_pending_offsets(consumer, offset_plan):
    for topic_partition, data in offset_plan.items():
        consumer.seek(topic_partition, data["start_offset"])


def process_message(cursor, message_value):
    payload = message_value or {}
    if not payload:
        return None, "skipped", "Missing Debezium payload"

    operation = payload.get("op")
    customer_id = None

    if operation in {"c", "u", "r"}:
        customer_id = (payload.get("after") or {}).get("id")
        upsert_customer(cursor, payload)
        return customer_id, "success", None

    if operation == "d":
        customer_id = (payload.get("before") or {}).get("id")
        delete_customer(cursor, payload)
        return customer_id, "success", None

    return customer_id, "skipped", f"Unsupported operation: {operation}"


def drain_pending_messages(consumer, total_pending, poll_timeout_ms):
    drained_messages = []

    while len(drained_messages) < total_pending:
        polled = consumer.poll(timeout_ms=poll_timeout_ms, max_records=total_pending)
        if not polled:
            break

        for topic_partition in sorted(polled.keys(), key=lambda item: item.partition):
            drained_messages.extend(polled[topic_partition])

    return drained_messages


def commit_batch_offsets(consumer, messages):
    offsets_to_commit = {}
    for message in messages:
        topic_partition = TopicPartition(message.topic, message.partition)
        next_offset = message.offset + 1
        current = offsets_to_commit.get(topic_partition)
        if current is None or next_offset > current.offset:
            offsets_to_commit[topic_partition] = OffsetAndMetadata(next_offset, None)

    if offsets_to_commit:
        consumer.commit(offsets=offsets_to_commit)

    return len(offsets_to_commit)


def process_batch(
    connection,
    consumer,
    process_run_id,
    process_id,
    batch_messages,
    mode,
    threshold_k,
    iceberg_table=None,
):
    committed_partition_count = 0

    with connection.cursor() as cursor:
        for message in batch_messages:
            payload = message.value or {}
            after = payload.get("after") or {}
            before = payload.get("before") or {}
            customer_id = after.get("id") or before.get("id")
            operation = payload.get("op")

            log_extracted_event(
                cursor,
                process_run_id=process_run_id,
                process_id=process_id,
                topic_name=message.topic,
                partition_id=message.partition,
                offset_value=message.offset,
                operation=operation,
                customer_id=customer_id,
                extract_mode=mode,
                payload=message.value,
            )

            if mode == "FULL_ETL":
                processed_customer_id, status, error_message = process_message(
                    cursor, message.value
                )
                log_etl_event(
                    cursor,
                    message.topic,
                    operation or "?",
                    processed_customer_id,
                    status,
                    error_message,
                )
            else:
                log_etl_event(
                    cursor,
                    message.topic,
                    operation or "?",
                    customer_id,
                    "extracted_only",
                    f"Waiting for more events to reach K={threshold_k}.",
                )

        # --- Iceberg Append ---
        if mode == "FULL_ETL" and iceberg_table is not None:
            try:
                count, new_cols = append_batch_to_iceberg(
                    iceberg_table, batch_messages, log_fn=log
                )
                if new_cols:
                    log(f"[SCHEMA EVOLUTION] Cac cot moi da duoc them: {new_cols}")
            except Exception as iceberg_err:
                log(f"[ICEBERG ERROR] Loi khi append vao Iceberg: {iceberg_err}")

        log_scheduled_process(
            cursor,
            process_run_id=process_run_id,
            process_id=process_id,
            topic_name=settings.KAFKA_TOPIC,
            mode=mode,
            planned_event_count=len(batch_messages),
            processed_event_count=len(batch_messages),
            committed_offset_count=0,
            status="completed" if mode == "FULL_ETL" else "staged",
            note=(
                "Committed offsets after full ETL batch."
                if mode == "FULL_ETL"
                else "Offsets not committed. Events remain pending for a future run."
            ),
        )
        connection.commit()

    if mode == "FULL_ETL":
        committed_partition_count = commit_batch_offsets(consumer, batch_messages)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE scheduled_process_log
            SET committed_offset_count = %s
            WHERE process_run_id = %s AND process_id = %s
            """,
            (committed_partition_count, process_run_id, process_id),
        )
        connection.commit()


def chunk_messages(messages, batch_size):
    for index in range(0, len(messages), batch_size):
        yield messages[index : index + batch_size]


def run_etl_job(connection, consumer, k, poll_timeout_ms, timeout_t=None, iceberg_table=None):
    process_run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    
    topic_partitions = get_topic_partitions(consumer)
    if not topic_partitions:
        log(f"Topic {settings.KAFKA_TOPIC} chua ton tai hoac chua co du lieu. Dang cho...")
        return

    total_pending, offset_plan = get_pending_offsets(consumer, topic_partitions)

    log("="*50)
    log(f"Kiem tra Kafka: N pending = {total_pending} / Threshold K = {k}")

    if total_pending == 0:
        log("Khong co event nao dang cho de xu ly.")
        return

    seek_to_pending_offsets(consumer, offset_plan)
    drained_messages = drain_pending_messages(
        consumer, total_pending, poll_timeout_ms
    )

    if len(drained_messages) != total_pending:
        log(
            f"Canh bao: Kafka hien co {total_pending} event pending "
            f"nhung chi doc duoc {len(drained_messages)} event trong lan poll nay."
        )

    log(f"Da doc {len(drained_messages)} event that tu Kafka.")

    for process_id, batch in enumerate(chunk_messages(drained_messages, k), start=1):
        mode = "EXTRACT_ONLY"
        if len(batch) == k:
            mode = "FULL_ETL"
        elif timeout_t is not None:
            oldest_ts_ms = min((msg.timestamp for msg in batch if msg.timestamp), default=None)
            if oldest_ts_ms:
                now_ms = time.time() * 1000
                if (now_ms - oldest_ts_ms) >= (timeout_t * 1000):
                    mode = "FULL_ETL"

        log(
            f"Process {process_id}: batch_size={len(batch)} mode={mode}"
        )
        process_batch(
            connection,
            consumer,
            process_run_id,
            process_id,
            batch,
            mode,
            k,
            iceberg_table=iceberg_table,
        )

        if mode == "FULL_ETL":
            log(f"Process {process_id} da chay du ETL va commit offset.")
        else:
            log(
                f"Process {process_id} chi Extract. Offset chua commit de cho lan chay sau."
            )

    log("Hoan tat mot dot ETL pipeline.")


def main():
    args = parse_args()
    if args.k <= 0:
        raise ValueError("K must be > 0")

    log(f"Khoi dong Scheduled Warehouse Loader. Dang doi database va Kafka...")

    # --- Khởi tạo Iceberg Catalog & Table ---
    log("Dang khoi tao Iceberg Catalog (SQL Catalog -> PostgreSQL, Storage -> MinIO S3)...")
    iceberg_catalog = get_iceberg_catalog()
    ensure_iceberg_namespace(iceberg_catalog)
    iceberg_table = get_or_create_iceberg_table(iceberg_catalog)
    log(
        f"Iceberg table san sang: {settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_TABLE_NAME}"
        f" | Schema columns: {[f.name for f in iceberg_table.schema().fields]}"
    )

    with closing(get_postgres_connection()) as connection:
        ensure_schema(connection)

        with closing(build_consumer(args.group_id)) as consumer:
            log(f"San sang luon lang nghe topic: {settings.KAFKA_TOPIC}")

            # Thiết lập lịch trình dựa trên tham số truyền vào (ưu tiên giây -> phút -> giờ)
            if args.interval_seconds:
                log(f"Setup loop moi {args.interval_seconds} giay.")
                schedule.every(args.interval_seconds).seconds.do(
                    run_etl_job, connection, consumer, args.k, args.poll_timeout_ms, args.timeout_t, iceberg_table
                )
            elif args.interval_minutes:
                log(f"Setup loop moi {args.interval_minutes} phut.")
                schedule.every(args.interval_minutes).minutes.do(
                    run_etl_job, connection, consumer, args.k, args.poll_timeout_ms, args.timeout_t, iceberg_table
                )
            elif args.interval_hours:
                log(f"Setup loop moi {args.interval_hours} gio.")
                schedule.every(args.interval_hours).hours.do(
                    run_etl_job, connection, consumer, args.k, args.poll_timeout_ms, args.timeout_t, iceberg_table
                )
            else:
                log("Mac dinh setup loop moi 5 giay.")
                schedule.every(5).seconds.do(
                    run_etl_job, connection, consumer, args.k, args.poll_timeout_ms, args.timeout_t, iceberg_table
                )

            # Chạy ngay một lần đầu tiên để cover các events đang pending
            schedule.run_all()

            # Vòng lặp chặn thread chính
            while True:
                schedule.run_pending()
                time.sleep(1)


if __name__ == "__main__":
    main()
