# 電商分析資料倉儲 (E-Commerce Analytics Data Warehouse)

## 專案簡介

模擬一家電商公司的分析型資料倉儲，整合三種不同型態的資料來源（交易資料庫 CDC、使用者行為事件流、外部第三方 API），透過 Medallion 架構（Bronze/Silver/Gold）分層清洗，最終產出符合 Kimball 維度建模的星狀模型，供 BI 查詢與分析使用。

專案重點在於展示資料工程裡最常被考的三個核心能力：

1. **多來源資料整合**：CDC、事件流、批次 API 三種擷取模式並存
2. **緩慢變化維度處理（SCD）**：正確保留歷史版本，確保歷史財務數字不被覆蓋
3. **資料品質保證（Reconciliation）**：Gold 層數字必須能對回 Bronze 層原始資料，誤差在容許範圍內才允許往下游流動

## 系統架構

```
┌──────────────┐   ┌───────────────┐   ┌──────────────────┐
│  訂單/商品/客戶  │   │  Clickstream   │   │   外部 API 模擬     │
│ (PostgreSQL)   │   │ (瀏覽/加購事件)  │   │  (金流/物流狀態)     │
└──────┬───────┘   └──────┬────────┘   └──────┬───────────┘
       │ CDC (Debezium)    │ Kafka Producer     │ 排程批次 (Airflow)
       ▼                   ▼                    ▼
┌────────────────────────────────────────────────────────┐
│                   Bronze 層 (MinIO, Parquet)                │
│              原始資料，append-only，不可修改                    │
└───────────────────────┬──────────────────────────────────┘
                         ▼
┌────────────────────────────────────────────────────────┐
│               Silver 層 (Iceberg)                           │
│   清洗、去重、型別驗證、SCD Type 1 / Type 2 邏輯               │
└───────────────────────┬──────────────────────────────────┘
                         ▼
┌────────────────────────────────────────────────────────┐
│               Gold 層 (dbt + 星狀模型)                        │
│   dim_products (SCD2) / dim_customers (SCD1) / dim_date      │
│   fact_order_items / fact_clickstream                        │
└───────────────────────┬──────────────────────────────────┘
                         ▼
                 資料品質驗證 (dbt tests / reconciliation)
                         ▼
                  BI 查詢層 (DuckDB + Metabase)
```

## 技術選型

| 環節 | 工具 | 說明 |
|---|---|---|
| 模擬交易資料 | Python + Faker | 產生訂單、商品、客戶假資料，並模擬商品改價事件 |
| 交易資料庫 | PostgreSQL | 啟用邏輯複製 (logical replication) 供 CDC 使用 |
| CDC | Debezium | 監聽 PostgreSQL WAL，捕捉 INSERT/UPDATE 事件 |
| 事件流 | Kafka | 承接 Clickstream 事件與 CDC 變更事件 |
| Bronze/Silver 儲存 | MinIO (S3 相容) + Apache Iceberg | 分層儲存，Iceberg 提供 schema evolution 與 time travel |
| 轉換 / 建模 | dbt + DuckDB | Gold 層星狀模型建構與資料品質測試 |
| 排程 | Apache Airflow | 串接批次 API 拉取、觸發 dbt run |
| 資料品質 | dbt tests + 自訂 reconciliation 測試 | 確保 Gold 層與 Bronze 層數字一致 |
| 查詢展示 | Metabase | 視覺化營收趨勢與 SCD Type 2 正確性對比 |

## 資料模型

### 來源層（PostgreSQL）
```
orders        (order_id, customer_id, order_date, status)
order_items   (order_item_id, order_id, product_id, quantity, unit_price)
products      (product_id, name, category, list_price, updated_at)
customers     (customer_id, name, email, city, segment, updated_at)
```

### Gold 層（星狀模型）
```
dim_products    (product_sk, product_id, name, category, list_price,
                 is_current, valid_from, valid_to)      -- SCD Type 2

dim_customers   (customer_sk, customer_id, name, city, segment)  -- SCD Type 1

dim_date        (date_sk, date, year, month, day, is_weekend, quarter)

fact_order_items (order_item_id, order_id, product_sk, customer_sk,
                  date_sk, quantity, unit_price, revenue)

fact_clickstream (event_id, customer_sk, product_sk, date_sk,
                   event_type, session_id, event_timestamp)
```

## 核心設計決策

### 1. 為什麼商品用 SCD Type 2，客戶用 SCD Type 1

商品價格變動會直接影響歷史訂單的財務正確性——三個月前的訂單金額必須反映當時的價格，不能因為現在改價就往回改。客戶的城市、姓名變動則通常只需要「現在」的狀態，用於現在的行銷分群，歷史版本對大部分分析沒有意義。判斷原則：**這個欄位的變動，是否會影響歷史事實的正確性計算**。

### 2. Point-in-time Join

`fact_order_items` 寫入時，不是存 `product_id`，而是根據訂單時間去查詢當時哪個 `product_sk` 版本生效，直接寫入正確的代理鍵。這樣後續查詢完全不需要額外的時間判斷邏輯，歷史訂單自動對應到當時的正確價格。

### 3. 三種資料擷取模式並存的理由

- **CDC**：適用於自己有資料庫存取權的系統（訂單、商品、客戶），可做到近乎即時且不需修改應用程式碼
- **Kafka 事件流**：適用於本質上就是持續發生的行為事件（使用者瀏覽、加購），不是「資料庫裡一列被改了」的形式
- **排程批次拉 API**：適用於沒有資料庫存取權的外部第三方系統（金流、物流商），只能定期查詢對方開放的介面

### 4. Reconciliation（資料品質防線）

每次 Gold 層跑完，驗證 `fact_order_items` 的營收加總與 Bronze 層原始資料加總的誤差是否在容許範圍內（例如 0.01%）。若超標則擋住，不讓本次結果流向下游——寧可提供昨天的舊資料，也不提供今天可能有誤的資料。

## 專案結構

```
ecommerce-data-warehouse/
├── data-generator/        # Faker 模擬資料生成腳本
├── cdc/                   # Debezium 設定
├── streaming/             # Kafka producer/consumer（clickstream）
├── ingestion/             # 批次 API 拉取腳本（金流/物流模擬）
├── dbt/                   # dbt models（Silver SCD 邏輯 + Gold 星狀模型）
│   ├── models/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/             # reconciliation 與 schema 測試
├── dags/                  # Airflow DAG
├── docker-compose.yml
└── README.md
```

## 開發階段

- [ ] **階段 1**：Faker 資料生成 + PostgreSQL + Bronze 層初始快照（一次性批次匯出，補齊 CDC 監聽前已存在的資料）
- [ ] **階段 2**：導入 Debezium + Kafka，CDC 事件即時進 Bronze；補上 Clickstream 事件流
- [ ] **階段 3**：Silver 層 SCD Type 1 / Type 2 邏輯，含邊界案例測試（同日連續改價、CDC 延遲到達）
- [ ] **階段 4**：dbt 建 Gold 層星狀模型 + point-in-time join + reconciliation 測試
- [ ] **階段 5**：Airflow 排程整合（含批次 API 拉取）+ Metabase dashboard

## 已知邊界案例（開發時需驗證）

- 同一天內對同一商品連續改價兩次，`valid_from`/`valid_to` 是否會重疊或產生 gap
- 訂單時間剛好等於某個商品版本的 `valid_from` 時，該歸屬哪個版本（`>=` vs `>` 的邊界判斷）
- CDC 事件因網路延遲晚到，時間戳記與業務邏輯時間不一致時如何處理

## License

MIT