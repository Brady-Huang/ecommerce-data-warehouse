CREATE TABLE customers(
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    segment VARCHAR(100) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE products(
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(100) NOT NULL,
    list_price DECIMAL(10,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE orders(
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL REFERENCES customers(customer_id),
    order_date TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(20) NOT NULL DEFAULT 'created'
);

CREATE TABLE order_items(
    order_item_id VARCHAR(20) PRIMARY KEY,
    order_id VARCHAR(20) NOT NULL REFERENCES orders(order_id),
    product_id VARCHAR(20) NOT NULL  REFERENCES products(product_id),
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL
);

-- 供 Debezium CDC 監聽使用
CREATE PUBLICATION ecom_publication FOR TABLE orders, order_items, products, customers;

-- REPLICA IDENTITY FULL：確保 UPDATE 事件的 before 欄位包含完整的舊資料
-- （預設的 REPLICA IDENTITY DEFAULT 只用主鍵識別變更列，不會帶出舊值）
ALTER TABLE orders REPLICA IDENTITY FULL;
ALTER TABLE order_items REPLICA IDENTITY FULL;
ALTER TABLE products REPLICA IDENTITY FULL;
ALTER TABLE customers REPLICA IDENTITY FULL;
