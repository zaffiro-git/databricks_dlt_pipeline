import dlt
from pyspark.sql import functions as F

#LANDING=spark.conf.get("landing_path")
LANDING = spark.conf.get("landing_path").strip().rstrip("/")
CUSTOMERS_SCHEMA = (
    "customer_id STRING, first_name STRING, last_name STRING, email STRING, city STRING, "
    "state STRING, signup_date DATE, op STRING, updated_at TIMESTAMP"
)
PRODUCTS_SCHEMA = (
    "product_id STRING, product_name STRING, category STRING, price DOUBLE, updated_at TIMESTAMP"
)
ORDERS_SCHEMA = (
    "order_id STRING, customer_id STRING, order_ts TIMESTAMP, status STRING, "
    "payment_method STRING, currency STRING, updated_at TIMESTAMP, "
    "items ARRAY<STRUCT<product_id: STRING, quantity: INT, unit_price: DOUBLE>>"
)

def _read_landing(source: str, fmt: str, schema: str, **extra):
    reader = (
        spark.readStream.format("cloudFiles") 
        .option("cloudFiles.format", fmt)
        .option("rescuedDataColumn","_rescued_data")
        .schema(schema)
        )
    for k,v in extra.items():
        reader=reader.option(k,v)
    return(
        reader.load(f"{LANDING}/{source}")
        .withColumn("ingested_At",F.current_timestamp())
        .withColumn("source_file",F.col("_metadata.file_path"))
    )
@dlt.table(
    name="bronze_customers_cdc",
    comment="Raw customer change feed (I/U/D) from the CRM export."
    )
def bronze_customers_cdc():
    return _read_landing("customers","csv",CUSTOMERS_SCHEMA,header="True")

@dlt.table(
    name="bronze_products",
    comment="Raw product catalog snapshots / price changes."
)
def bronze_products():
    return _read_landing("products", "csv", PRODUCTS_SCHEMA, header="true")

@dlt.table(
    name="bronze_orders",
    comment="Raw order events (nested items) from the order service. May contain duplicates."
)
def bronze_orders():
    return _read_landing("orders", "json", ORDERS_SCHEMA)