from pyspark.sql import SparkSession, functions as F

PG_URL = "jdbc:postgresql://postgres:5432/bigdataSpark"
PG_PROPS = {"user": "bigdata", "password": "bddb", "driver": "org.postgresql.Driver"}

def main():
    spark = (
        SparkSession.builder
        .appName("etl_to_star")
        .config("spark.jars", "/home/jovyan/work/jars/postgresql.jar,/home/jovyan/work/jars/clickhouse.jar")
        .getOrCreate()
    )

    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = spark.read.jdbc(url=PG_URL, table="stg.mock_data", properties=PG_PROPS)

    df = df.withColumn("sale_dt", F.to_date(F.col("sale_date"), "MM/dd/yyyy")) \
           .withColumn("release_dt", F.to_date(F.col("product_release_date"), "MM/dd/yyyy")) \
           .withColumn("expiry_dt",  F.to_date(F.col("product_expiry_date"), "MM/dd/yyyy"))

    customer_key = F.xxhash64(F.coalesce(F.col("customer_email"), F.lit("")))
    seller_key   = F.xxhash64(F.coalesce(F.col("seller_email"), F.lit("")))
    store_key    = F.xxhash64(F.coalesce(F.col("store_email"), F.lit("")))
    supplier_key = F.xxhash64(F.coalesce(F.col("supplier_email"), F.lit("")))

    product_nk = F.concat_ws("||",
                             F.coalesce(F.col("product_name"), F.lit("")),
                             F.coalesce(F.col("product_category"), F.lit("")),
                             F.coalesce(F.col("product_brand"), F.lit("")),
                             F.coalesce(F.col("supplier_email"), F.lit("")))
    product_key = F.xxhash64(product_nk)

    dim_customer = (
        df.select(
            customer_key.alias("customer_key"),
            "customer_email",
            F.col("customer_first_name").alias("first_name"),
            F.col("customer_last_name").alias("last_name"),
            F.col("customer_age").alias("age"),
            F.col("customer_country").alias("country"),
            F.col("customer_postal_code").alias("postal_code"),
            F.col("customer_pet_type").alias("pet_type"),
            F.col("customer_pet_name").alias("pet_name"),
            F.col("customer_pet_breed").alias("pet_breed"),
        )
        .where(F.col("customer_email").isNotNull())
        .dropDuplicates(["customer_key"])
    )

    dim_seller = (
        df.select(
            seller_key.alias("seller_key"),
            "seller_email",
            F.col("seller_first_name").alias("first_name"),
            F.col("seller_last_name").alias("last_name"),
            F.col("seller_country").alias("country"),
            F.col("seller_postal_code").alias("postal_code"),
        )
        .where(F.col("seller_email").isNotNull())
        .dropDuplicates(["seller_key"])
    )

    dim_store = (
        df.select(
            store_key.alias("store_key"),
            "store_email",
            "store_name",
            F.col("store_location").alias("location"),
            F.col("store_city").alias("city"),
            F.col("store_state").alias("state"),
            F.col("store_country").alias("country"),
            F.col("store_phone").alias("phone"),
        )
        .where(F.col("store_email").isNotNull())
        .dropDuplicates(["store_key"])
    )

    dim_supplier = (
        df.select(
            supplier_key.alias("supplier_key"),
            "supplier_email",
            "supplier_name",
            F.col("supplier_contact").alias("contact_name"),
            F.col("supplier_phone").alias("phone"),
            F.col("supplier_address").alias("address"),
            F.col("supplier_city").alias("city"),
            F.col("supplier_country").alias("country"),
        )
        .where(F.col("supplier_email").isNotNull())
        .dropDuplicates(["supplier_key"])
    )

    dim_product = (
        df.select(
            product_key.alias("product_key"),
            F.col("product_name").alias("product_name"),
            F.col("product_category").alias("category"),
            F.col("product_brand").alias("brand"),
            F.col("product_material").alias("material"),
            F.col("product_color").alias("color"),
            F.col("product_size").alias("size"),
            F.col("pet_category").alias("pet_category"),
            F.col("product_price").alias("price"),
            F.col("product_weight").alias("weight"),
            F.col("product_description").alias("description"),
            F.col("product_rating").alias("rating"),
            F.col("product_reviews").alias("reviews"),
            F.col("release_dt").alias("release_date"),
            F.col("expiry_dt").alias("expiry_date"),
            supplier_key.alias("supplier_key"),
        )
        .where(F.col("product_name").isNotNull())
        .dropDuplicates(["product_key"])
    )

    dim_date = (
        df.select(F.col("sale_dt").alias("full_date"))
          .where(F.col("full_date").isNotNull())
          .dropDuplicates(["full_date"])
          .withColumn("year", F.year("full_date"))
          .withColumn("quarter", F.quarter("full_date"))
          .withColumn("month", F.month("full_date"))
          .withColumn("day", F.dayofmonth("full_date"))
          .withColumn("date_key",
                      (F.col("year")*10000 + F.col("month")*100 + F.col("day")).cast("int"))
          .select("date_key","full_date","year","quarter","month","day")
    )

    fact = (
        df.where(F.col("sale_dt").isNotNull())
          .withColumn("date_key", (F.year("sale_dt")*10000 + F.month("sale_dt")*100 + F.dayofmonth("sale_dt")).cast("int"))
          .select(
              "date_key",
              customer_key.alias("customer_key"),
              seller_key.alias("seller_key"),
              store_key.alias("store_key"),
              product_key.alias("product_key"),
              "sale_quantity",
              "sale_total_price",
              "sale_customer_id",
              "sale_seller_id",
              "sale_product_id",
          )
    )

    def write_pg(df_out, table):
        (df_out.write
            .mode("overwrite")
            .option("truncate", "true")
            .jdbc(url=PG_URL, table=table, properties=PG_PROPS))

    write_pg(dim_customer, "dw.dim_customer")
    write_pg(dim_seller,   "dw.dim_seller")
    write_pg(dim_store,    "dw.dim_store")
    write_pg(dim_supplier, "dw.dim_supplier")
    write_pg(dim_product,  "dw.dim_product")
    write_pg(dim_date,     "dw.dim_date")
    write_pg(fact,         "dw.fact_sales")

    spark.stop()

if __name__ == "__main__":
    main()
