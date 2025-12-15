from pyspark.sql import SparkSession, functions as F

PG_URL = "jdbc:postgresql://postgres:5432/bigdataSpark"
PG_PROPS = {"user": "bigdata", "password": "bddb", "driver": "org.postgresql.Driver"}

CH_URL = "jdbc:clickhouse://clickhouse:8123/marts"
CH_PROPS = {"user": "spark", "password": "spark", "driver": "com.clickhouse.jdbc.ClickHouseDriver"}

def main():
    spark = (
        SparkSession.builder
        .appName("marts_to_clickhouse")
        .config("spark.jars", "/home/jovyan/work/jars/postgresql.jar,/home/jovyan/work/jars/clickhouse.jar")
        .getOrCreate()
    )

    fact = spark.read.jdbc(PG_URL, "dw.fact_sales", properties=PG_PROPS)
    prod = spark.read.jdbc(PG_URL, "dw.dim_product", properties=PG_PROPS)
    cust = spark.read.jdbc(PG_URL, "dw.dim_customer", properties=PG_PROPS)
    store = spark.read.jdbc(PG_URL, "dw.dim_store", properties=PG_PROPS)
    supp = spark.read.jdbc(PG_URL, "dw.dim_supplier", properties=PG_PROPS)
    dt = spark.read.jdbc(PG_URL, "dw.dim_date", properties=PG_PROPS)

    mart_products = (
        fact.join(prod, "product_key")
            .groupBy("product_key", "product_name", "category")
            .agg(
                F.sum("sale_total_price").cast("double").alias("revenue"),
                F.sum("sale_quantity").cast("long").alias("qty"),
                F.avg("rating").cast("double").alias("avg_rating"),
                F.max("reviews").cast("long").alias("reviews"),
            )
            .orderBy(F.desc("revenue"))
    )

    mart_customers = (
        fact.join(cust, "customer_key")
            .groupBy("customer_key", "customer_email", "country")
            .agg(
                F.sum("sale_total_price").cast("double").alias("revenue"),
                F.count(F.lit(1)).cast("long").alias("orders"),
            )
            .withColumn("avg_check", (F.col("revenue") / F.col("orders")).cast("double"))
            .orderBy(F.desc("revenue"))
    )

    mart_time = (
        fact.join(dt, "date_key")
            .groupBy("year", "month")
            .agg(
                F.count(F.lit(1)).cast("long").alias("orders"),
                F.sum("sale_total_price").cast("double").alias("revenue"),
            )
            .withColumn("avg_order", (F.col("revenue") / F.col("orders")).cast("double"))
            .orderBy("year", "month")
    )

    mart_stores = (
        fact.join(store, "store_key")
            .groupBy("store_key", "store_name", "city", "country")
            .agg(
                F.count(F.lit(1)).cast("long").alias("orders"),
                F.sum("sale_total_price").cast("double").alias("revenue"),
            )
            .withColumn("avg_check", (F.col("revenue") / F.col("orders")).cast("double"))
            .orderBy(F.desc("revenue"))
    )

    mart_suppliers = (
        fact.join(prod.select("product_key", "supplier_key", "price"), "product_key")
            .join(supp.select("supplier_key", "supplier_name", "country"), "supplier_key")
            .groupBy("supplier_key", "supplier_name", "country")
            .agg(
                F.sum("sale_total_price").cast("double").alias("revenue"),
                F.avg("price").cast("double").alias("avg_product_price"),
            )
            .orderBy(F.desc("revenue"))
    )

    mart_quality = (
        fact.join(prod.select("product_key","product_name","rating","reviews"), "product_key")
            .groupBy("product_key","product_name","rating","reviews")
            .agg(
                F.sum("sale_quantity").cast("long").alias("qty"),
                F.sum("sale_total_price").cast("double").alias("revenue"),
            )
            .orderBy(F.desc("rating"), F.desc("reviews"))
    )

    def write_ch(df_out, table, order_by):
        (df_out.write
            .mode("overwrite")
            .option("truncate", "true")
            .option("createTableOptions", f"ENGINE=MergeTree() ORDER BY ({order_by})")
            .jdbc(url=CH_URL, table=table, properties=CH_PROPS))


    write_ch(mart_products,  "mart_products",  "product_key")
    write_ch(mart_customers, "mart_customers", "customer_key")
    write_ch(mart_time,      "mart_time",      "year, month")
    write_ch(mart_stores,    "mart_stores",    "store_key")
    write_ch(mart_suppliers, "mart_suppliers", "supplier_key")
    write_ch(mart_quality,   "mart_quality",   "product_key")

    spark.stop()

if __name__ == "__main__":
    main()
