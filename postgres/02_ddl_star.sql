CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_sales;
DROP TABLE IF EXISTS dw.dim_date;
DROP TABLE IF EXISTS dw.dim_product;
DROP TABLE IF EXISTS dw.dim_customer;
DROP TABLE IF EXISTS dw.dim_seller;
DROP TABLE IF EXISTS dw.dim_store;
DROP TABLE IF EXISTS dw.dim_supplier;

CREATE TABLE dw.dim_customer (
  customer_key     bigint PRIMARY KEY,
  customer_email   text,
  first_name       text,
  last_name        text,
  age              int,
  country          text,
  postal_code      text,
  pet_type         text,
  pet_name         text,
  pet_breed        text
);

CREATE TABLE dw.dim_seller (
  seller_key     bigint PRIMARY KEY,
  seller_email   text,
  first_name     text,
  last_name      text,
  country        text,
  postal_code    text
);

CREATE TABLE dw.dim_store (
  store_key     bigint PRIMARY KEY,
  store_email   text,
  store_name    text,
  location      text,
  city          text,
  state         text,
  country       text,
  phone         text
);

CREATE TABLE dw.dim_supplier (
  supplier_key   bigint PRIMARY KEY,
  supplier_email text,
  supplier_name  text,
  contact_name   text,
  phone          text,
  address        text,
  city           text,
  country        text
);

CREATE TABLE dw.dim_product (
  product_key          bigint PRIMARY KEY,
  product_name         text,
  category             text,
  brand                text,
  material             text,
  color                text,
  size                 text,
  pet_category         text,
  price                numeric(12,2),
  weight               numeric(12,3),
  description          text,
  rating               numeric(3,2),
  reviews              int,
  release_date         date,
  expiry_date          date,
  supplier_key         bigint
);

CREATE TABLE dw.dim_date (
  date_key   int PRIMARY KEY, -- yyyymmdd
  full_date  date,
  year       int,
  quarter    int,
  month      int,
  day        int
);

CREATE TABLE dw.fact_sales (
  sales_key        bigserial PRIMARY KEY,
  date_key         int,
  customer_key     bigint,
  seller_key       bigint,
  store_key        bigint,
  product_key      bigint,
  sale_quantity    int,
  sale_total_price numeric(12,2),
  sale_customer_id int,
  sale_seller_id   int,
  sale_product_id  int
);
