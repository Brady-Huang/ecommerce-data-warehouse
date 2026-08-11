"""
Bronze 層初始快照 —— 一次性批次匯出

把 PostgreSQL 目前四張表的資料，整批匯出成 Parquet 檔案，
上傳到 MinIO 的 bronze bucket。

這一步只需要跑一次，目的是補齊「CDC 開始監聽之前」已經存在的資料。
之後階段 2 導入 CDC 後，負責處理「之後」發生的所有變更，
兩者互補，不是重複工作。
"""

import os
import logging

import pandas as pd
import psycopg2
import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_export")

# 從環境變數讀取連線資訊，第二個參數是本機測試時的預設值
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
    "user": os.environ.get("POSTGRES_USER", "ecom"),
    "password": os.environ.get("POSTGRES_PASSWORD", "ecom_pw"),
    "dbname": os.environ.get("POSTGRES_DB", "ecom_source"),
}

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "bronze"

TABLES = ["customers", "products", "orders", "order_items"]

# 暫存 parquet 檔案的本機資料夾（上傳完可以清掉，不需要留著）
TMP_DIR = "tmp_export"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def export_table_to_parquet(conn, table_name):
    """
    讀取整張表，轉成 DataFrame，存成本機暫存的 parquet 檔案。
    回傳本機檔案路徑，供後續上傳使用。
    """
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    local_path = os.path.join(TMP_DIR, f"{table_name}.parquet")
    df.to_parquet(local_path, index=False)
    log.info(f"{table_name}：讀取 {len(df)} 筆資料，已存成 {local_path}")
    return local_path


def upload_to_minio(client, local_path, bucket, key):
    """
    把本機的 parquet 檔案上傳到 MinIO 指定的 bucket/key。
    """
    client.upload_file(local_path, bucket, key)
    log.info(f"已上傳 {local_path} -> s3://{bucket}/{key}")


def main():
    os.makedirs(TMP_DIR, exist_ok=True)

    conn = psycopg2.connect(**DB_CONFIG)
    client = get_minio_client()

    for table in TABLES:
        local_path = export_table_to_parquet(conn, table)
        # 照表名分資料夾存放，例如 bronze/customers/customers.parquet
        key = f"{table}/{table}.parquet"
        upload_to_minio(client, local_path, BUCKET_NAME, key)
        os.remove(local_path)
        log.info(f"已清除本機暫存檔 {local_path}")
    
    conn.close()
    log.info("Bronze 層初始快照完成")


if __name__ == "__main__":
    main()