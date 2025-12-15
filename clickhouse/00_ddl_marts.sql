CREATE DATABASE IF NOT EXISTS marts;

DROP TABLE IF EXISTS marts.mart_products;
DROP TABLE IF EXISTS marts.mart_customers;
DROP TABLE IF EXISTS marts.mart_time;
DROP TABLE IF EXISTS marts.mart_stores;
DROP TABLE IF EXISTS marts.mart_suppliers;
DROP TABLE IF EXISTS marts.mart_quality;

CREATE TABLE marts.mart_products
(
  product_key Int64,
  product_name String,
  category String,
  revenue Float64,
  qty Int64,
  avg_rating Float64,
  reviews Int64
)
ENGINE = MergeTree
ORDER BY (revenue, product_key);

CREATE TABLE marts.mart_customers
(
  customer_key Int64,
  customer_email String,
  country String,
  revenue Float64,
  orders Int64,
  avg_check Float64
)
ENGINE = MergeTree
ORDER BY (revenue, customer_key);

CREATE TABLE marts.mart_time
(
  year Int32,
  month Int32,
  orders Int64,
  revenue Float64,
  avg_order Float64
)
ENGINE = MergeTree
ORDER BY (year, month);

CREATE TABLE marts.mart_stores
(
  store_key Int64,
  store_name String,
  city String,
  country String,
  orders Int64,
  revenue Float64,
  avg_check Float64
)
ENGINE = MergeTree
ORDER BY (revenue, store_key);

CREATE TABLE marts.mart_suppliers
(
  supplier_key Int64,
  supplier_name String,
  country String,
  revenue Float64,
  avg_product_price Float64
)
ENGINE = MergeTree
ORDER BY (revenue, supplier_key);

CREATE TABLE marts.mart_quality
(
  product_key Int64,
  product_name String,
  rating Float64,
  reviews Int64,
  qty Int64,
  revenue Float64
)
ENGINE = MergeTree
ORDER BY (rating, reviews);
