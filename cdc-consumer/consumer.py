"""
CDC Consumer —— 把 Debezium 送進 Kafka 的變更事件，批次寫入 Bronze 層 (MinIO)

設計理念：
- 混合批次觸發策略：累積達到 BATCH_SIZE 筆，或超過 BATCH_INTERVAL_SECONDS 秒，
  兩個條件先到就寫入一次 Parquet 檔案，避免產生大量小檔案，也避免資料延遲過久才落地。
- Decimal 型別（如 list_price）在 Debezium 訊息裡是 Base64 編碼的 bytes，
  這裡會解碼還原成可讀的數字，Bronze 層存的應該是「看得懂」的資料，
  不是原始的編碼格式。
- Bronze 層依然遵守 append-only 原則：每一批寫入都是「新增」一個檔案，
  不會覆蓋或修改之前寫入的檔案，即使同一張表的 CDC 事件持續產生。
"""

import os
import json
import base64
import logging
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import pandas as pd
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cdc_consumer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bronze"

# 監聽的四個 CDC topic，對應到來源的四張表
TOPICS = [
    "ecom.public.customers",
    "ecom.public.products",
    "ecom.public.orders",
    "ecom.public.order_items",
]

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", 50))
BATCH_INTERVAL_SECONDS = int(os.environ.get("BATCH_INTERVAL_SECONDS", 10))

TMP_DIR = "tmp_cdc"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def decode_decimal(value, scale):
    """
    Debezium 對 PostgreSQL 的 NUMERIC/DECIMAL 型別，
    預設會編碼成 Base64 的 bytes（org.apache.kafka.connect.data.Decimal）。
    這裡把它解碼還原成一般的浮點數，方便 Bronze 層直接存成可讀資料。
    """
    if value is None:
        return None
    raw_bytes = base64.b64decode(value)
    unscaled = int.from_bytes(raw_bytes, byteorder="big", signed=True)
    return unscaled / (10 ** scale)


def parse_event(message_value, table_name):
    """
    解析一筆 Debezium CDC 事件，抽取出 op（操作類型）、
    改動後的完整欄位內容（after），並處理 Decimal 型別解碼。

    回傳一個 flat dict，方便之後轉成 DataFrame 存 Parquet。
    """
    payload = message_value.get("payload", {})
    op = payload.get("op")  # 'r'=snapshot讀取, 'c'=insert, 'u'=update, 'd'=delete
    after = payload.get("after")
    before = payload.get("before")

    # DELETE 事件沒有 after，只有 before；其他情況以 after 為主要內容
    record = after if after is not None else before

    if record is None:
        return None

    # list_price 是目前唯一的 Decimal 欄位，這裡做解碼處理
    if table_name == "products" and "list_price" in record and record["list_price"] is not None:
        record["list_price"] = decode_decimal(record["list_price"], scale=2)

    if table_name == "order_items" and "unit_price" in record and record["unit_price"] is not None:
        record["unit_price"] = decode_decimal(record["unit_price"], scale=2)

    return {
        "_cdc_op": op,
        "_cdc_table": table_name,
        "_cdc_ts_ms": payload.get("ts_ms"),
        **record,
    }


def flush_batch(client, table_name, records):
    """
    把累積的一批記錄寫成一個 Parquet 檔案，上傳到 MinIO 的 bronze bucket。
    檔名帶時間戳記，確保每次 flush 都是「新增」一個檔案，符合 append-only 原則，
    不會覆蓋掉之前已經寫入的 CDC 事件記錄。
    """
    if not records:
        return

    os.makedirs(TMP_DIR, exist_ok=True)
    df = pd.DataFrame(records)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    filename = f"{table_name}_{timestamp}.parquet"
    local_path = os.path.join(TMP_DIR, filename)

    df.to_parquet(local_path, index=False)

    key = f"{table_name}/{filename}"
    client.upload_file(local_path, BUCKET_NAME, key)
    os.remove(local_path)

    log.info(f"[{table_name}] 寫入 {len(records)} 筆事件 -> s3://{BUCKET_NAME}/{key}")


def main():
    minio_client = get_minio_client()

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-writer",
        consumer_timeout_ms=BATCH_INTERVAL_SECONDS * 1000,
    )

    log.info(f"開始監聽 topics: {TOPICS}")
    log.info(f"批次策略：累積 {BATCH_SIZE} 筆，或每 {BATCH_INTERVAL_SECONDS} 秒，先到就寫入")

    # 每張表各自維護一個累積區，因為不同表的 schema 不同，不能混在一起寫成同一個 Parquet
    buffers = {table.split(".")[-1]: [] for table in TOPICS}

    while True:
        # consumer_timeout_ms 會讓這個迴圈最多等 BATCH_INTERVAL_SECONDS 秒，
        # 如果這段時間內沒有新訊息，會自然跳出，讓我們可以檢查是否該基於時間 flush。
        got_any_message = False
        for message in consumer:
            got_any_message = True
            table_name = message.topic.split(".")[-1]
            event = parse_event(message.value, table_name)

            if event is not None:
                buffers[table_name].append(event)

            if len(buffers[table_name]) >= BATCH_SIZE:
                flush_batch(minio_client, table_name, buffers[table_name])
                buffers[table_name] = []

        # consumer 逾時跳出（代表這段時間沒有新事件），
        # 這時候把所有還沒滿 BATCH_SIZE、但已經等了一段時間的資料，強制寫入一次
        for table_name, records in buffers.items():
            if records:
                flush_batch(minio_client, table_name, records)
                buffers[table_name] = []

        if not got_any_message:
            log.info("等待中，目前沒有新的 CDC 事件")


if __name__ == "__main__":
    main()