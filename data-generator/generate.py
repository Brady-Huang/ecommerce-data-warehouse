"""
電商模擬資料生成器

負責四件事：
1. Seed：初始化一批客戶與商品
2. 持續產生訂單（模擬交易持續發生）
3. 隨機模擬商品改價（用於驗證下游 SCD Type 2 邏輯）
4. 隨機模擬客戶資料變更（用於驗證下游 SCD Type 1 邏輯）

設計理念：
- order_items.unit_price 存的是「下單當時」商品的實際生效價格，
  這個欄位是 Bronze 層的「事實」，之後不管商品價格怎麼變，
  這筆歷史訂單的單價都不該被改變。
- 商品改價事件故意不做防呆（沒有擋同一天內被選到兩次），
  長時間跑下來一定會出現「同一天連續改價兩次」這種邊界案例，
  留給之後 Silver 層的 SCD Type 2 邏輯正確處理。
"""

import os
import time
import random
import logging
from datetime import datetime

import psycopg2
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generator")

fake = Faker("zh_TW")

# 從環境變數讀取連線資訊
DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "localhost"),
    "port": os.environ.get("POSTGRES_PORT", "5432"),
    "user": os.environ.get("POSTGRES_USER", "ecom"),
    "password": os.environ.get("POSTGRES_PASSWORD", "ecom_pw"),
    "dbname": os.environ.get("POSTGRES_DB", "ecom_source"),
}

NUM_CUSTOMERS = int(os.environ.get("NUM_CUSTOMERS", 200))
NUM_PRODUCTS = int(os.environ.get("NUM_PRODUCTS", 50))
ORDER_INTERVAL_SECONDS = float(os.environ.get("ORDER_INTERVAL_SECONDS", 2))
PRICE_CHANGE_PROBABILITY = float(os.environ.get("PRICE_CHANGE_PROBABILITY", 0.05))
CUSTOMER_UPDATE_PROBABILITY = float(os.environ.get("CUSTOMER_UPDATE_PROBABILITY", 0.02))

CATEGORIES = ["3C配件", "居家生活", "美妝保養", "食品飲料", "服飾", "運動戶外", "寵物用品", "書籍文具"]
SEGMENTS = ["new", "regular", "vip", "churned"]


def connect():
    """
    連線 PostgreSQL，帶重試機制。
    重試的原因：如果這個腳本包進 docker-compose 一起啟動，
    PostgreSQL container 可能還沒完全準備好接受連線，
    直接連會失敗，所以要重試幾次、每次間隔幾秒。
    """
    for attempt in range(10):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            log.info("成功連線 PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            log.warning(f"連線失敗，重試中 ({attempt + 1}/10): {e}")
            time.sleep(3)
    raise RuntimeError("多次重試後仍無法連上 PostgreSQL")


def seed_customers(conn, n=NUM_CUSTOMERS):
    """往 customers 表塞入 n 筆假資料，若已有資料則略過。"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customers")
        if cur.fetchone()[0] > 0:
            log.info("customers 表已有資料，略過 seed")
            return

        for i in range(n):
            cur.execute(
                """
                INSERT INTO customers (customer_id, name, email, city, segment)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    f"CUST-{i:05d}",
                    fake.name(),
                    fake.email(),
                    fake.city(),
                    fake.random_element(SEGMENTS),
                ),
            )
    conn.commit()
    log.info(f"已 seed {n} 筆客戶資料")


def seed_products(conn, n=NUM_PRODUCTS):
    """往 products 表塞入 n 筆假資料，若已有資料則略過。"""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM products")
        if cur.fetchone()[0] > 0:
            log.info("products 表已有資料，略過 seed")
            return

        for i in range(n):
            cur.execute(
                """
                INSERT INTO products (product_id, name, category, list_price)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    f"SKU-{i:05d}",
                    fake.catch_phrase(),
                    fake.random_element(CATEGORIES),
                    round(random.uniform(99, 4999), 2),
                ),
            )
    conn.commit()
    log.info(f"已 seed {n} 筆商品資料")


def load_ids(conn, table, id_col):
    """讀取某張表目前所有的主鍵值，回傳 list。"""
    with conn.cursor() as cur:
        cur.execute(f"SELECT {id_col} FROM {table}")
        return [row[0] for row in cur.fetchall()]


def load_product_prices(conn):
    """
    讀取目前所有商品的 list_price，維護在記憶體裡的字典。
    之後每次改價都同步更新這個字典，讓 create_order() 知道
    「現在」該用哪個價格產生訂單項目。
    """
    with conn.cursor() as cur:
        cur.execute("SELECT product_id, list_price FROM products")
        return {row[0]: float(row[1]) for row in cur.fetchall()}


def create_order(conn, customer_ids, product_prices, order_seq):
    """
    產生一筆訂單，內含 1~4 個 order_item。
    order_seq 是遞增計數器，用來組成不會重複的 order_id。
    """
    order_id = f"ORD-{order_seq:07d}"
    customer_id = random.choice(customer_ids)

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO orders (order_id, customer_id, status) VALUES (%s, %s, %s)",
            (order_id, customer_id, "created"),
        )

        num_items = random.randint(1, 4)
        chosen_products = random.sample(
            list(product_prices.keys()), min(num_items, len(product_prices))
        )
        for idx, product_id in enumerate(chosen_products):
            order_item_id = f"{order_id}-ITEM-{idx}"
            quantity = random.randint(1, 3)
            unit_price = product_prices[product_id]  # 下單當時實際生效的價格
            cur.execute(
                """
                INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (order_item_id, order_id, product_id, quantity, unit_price),
            )

    conn.commit()
    log.info(f"建立訂單 {order_id}，客戶 {customer_id}，{len(chosen_products)} 個商品項目")


def maybe_change_price(conn, product_prices):
    """
    有機率隨機挑一個商品改價。
    故意不擋「同一天內被選到兩次」，這是刻意設計的邊界案例，
    用來之後驗證 Silver 層 SCD Type 2 邏輯是否正確處理
    valid_from / valid_to 不重疊、不產生 gap。
    """
    if random.random() > PRICE_CHANGE_PROBABILITY:
        return

    product_id = random.choice(list(product_prices.keys()))
    old_price = product_prices[product_id]
    new_price = round(old_price * random.choice([0.8, 0.85, 0.9, 1.1, 1.15]), 2)

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE products SET list_price = %s, updated_at = %s WHERE product_id = %s",
            (new_price, datetime.now(), product_id),
        )
    conn.commit()

    product_prices[product_id] = new_price
    log.info(f"[改價事件] {product_id}: {old_price} -> {new_price}")


def maybe_update_customer(conn, customer_ids):
    """
    有機率隨機模擬客戶資料變更（搬家/分群變化）。
    用於驗證下游 SCD Type 1（直接覆蓋，不留歷史）邏輯。
    """
    if random.random() > CUSTOMER_UPDATE_PROBABILITY:
        return

    customer_id = random.choice(customer_ids)
    new_city = fake.city()
    new_segment = random.choice(SEGMENTS)

    with conn.cursor() as cur:
        cur.execute(
            "UPDATE customers SET city = %s, segment = %s, updated_at = %s WHERE customer_id = %s",
            (new_city, new_segment, datetime.now(), customer_id),
        )
    conn.commit()
    log.info(f"[客戶更新] {customer_id}: city={new_city}, segment={new_segment}")


def main():
    conn = connect()

    seed_customers(conn)
    seed_products(conn)

    customer_ids = load_ids(conn, "customers", "customer_id")
    product_prices = load_product_prices(conn)

    log.info(f"開始持續產生訂單，間隔 {ORDER_INTERVAL_SECONDS} 秒")

    order_seq = 1
    while True:
        try:
            create_order(conn, customer_ids, product_prices, order_seq)
            order_seq += 1
            maybe_change_price(conn, product_prices)
            maybe_update_customer(conn, customer_ids)
        except psycopg2.OperationalError as e:
            log.error(f"連線發生問題，重新連線: {e}")
            conn = connect()
        time.sleep(ORDER_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()