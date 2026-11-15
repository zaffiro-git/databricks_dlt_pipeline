from pyspark import pipelines as dp
from pyspark.sql import functions as F

VALID_STATUS="('PLACED','PAID','SHIPPED','DELIVERED','CANCELLED')"
NON_TECH_COLS=["ingested_At", "source_file", "_rescued_data"]

# ------------------customers (SCD2)-----------------

@dp.view(name="customer_cdc_clean",
         comment="Validated + normalised customer change events.")
@dp.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
@dp.expect_or_drop("op_valid", "op IN ('I','U','D')")
@dp.expect("email_present", "email IS NOT NULL")  # warn only: keep row, track metric
@dp.expect("email_format", "email IS NULL OR email LIKE '%_@_%._%'")

def customer_cdc_clean():
    email=F.lower(F.trim(F.col("email")))
    return (
        spark.readStream.table("bronze_customers_cdc")
        .withColumn("email",F.when(email=="",None).otherwise(email))
        .withColumn("first_name",F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",F.initcap(F.trim(F.col("last_name"))))
    )

dp.create_streaming_table(
    name="silver_customers",
    comment="Customer dimension with full history (SCD2). Current row has __END_AT IS NULL.",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_customers",
    source="customer_cdc_clean",
    keys=["customer_id"],
    sequence_by="updated_at",
    apply_as_deletes=F.expr("op='D'"),
    except_column_list=["op"] + NON_TECH_COLS,
    stored_as_scd_type=2
)

# ----------------- products (SCD1)-----------------------------------
@dp.view(name="products_clean")
@dp.expect_or_drop("product_id_not_null","product_id IS NOT NULL")
@dp.expect_or_drop("price_positive","price > 0")

def products_clean():
    return spark.readStream.table("bronze_products")

dp.create_streaming_table(
    name="silver_products",
    comment="Latest product attributes (SCD1).",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_products",
    source="products_clean",
    keys=["product_id"],
    sequence_by="updated_at",
    except_column_list=NON_TECH_COLS,
    stored_as_scd_type=1,
)

# ------------------------------------------- orders (SCD1, dedupe)

@dp.view(name="orders_clean")
@dp.expect_or_drop("order_id_not_null","order_id IS NOT NULL")
@dp.expect_or_drop("valid_status",f"status IN {VALID_STATUS}")
@dp.expect("customer_id_present", "customer_id IS NOT NULL")  # warn: orphan orders

def orders_clean():
    return spark.readStream.table("bronze_orders").select(
        "order_id", "customer_id", "order_ts", F.to_date("order_ts").alias("order_date"),
        "status", "payment_method", "currency", "updated_at", "ingested_At"
    )
dp.create_streaming_table(
    name="silver_orders",
    comment="One row per order, latest status wins. Duplicates removed.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target="silver_orders",
    source="orders_clean",
    keys=["order_id"],
    # struct tie-breaker makes late/duplicate deliveries deterministic
    sequence_by=F.struct("updated_at", "ingested_At"),
    except_column_list=["ingested_At"],
    stored_as_scd_type=1
)
# ----------------------------------------------------------------orderitems

ITEM_RULES={
    "product_id_not_null":"product_id is not null",
    "quantity_positive":"quantity> 0",
    "unit_price_non_negative":"unit_price>=0"
}
ITEM_QURANTINE="NOT COALESCE(" + "AND".join(f"({r})" for r in ITEM_RULES.values()) + ", false)"

@dp.view(name="order_items_raw")
def order_items_raw():
    return spark.readStream.table("bronze_orders").select(
        "order_id",F.posexplode("items").alias("line_no","item"),
        "updated_at", "ingested_At"
    ).select (
        "order_id", "line_no","updated_at","ingested_At",F.col("item.product_id").alias("product_id"),F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price")
    )

@dp.view(name="order_item_clean")
@dp.expect_all_or_drop(ITEM_RULES)
def order_item_clean():
    return spark.readStream.table("order_items_raw")

@dp.table(
    name="quarantine_order_items",
    comment="Order lines that failed validation. Review, fix upstream, replay.",
    table_properties={"quality": "quarantine"},
)

def quarantine_order_items():
    return spark.readStream.table("order_items_raw").where(ITEM_QURANTINE)

dp.create_streaming_table(
    name="silver_order_items",
    comment="One row per (order_id, line_no) with validated quantities and prices.",
    table_properties={"quality": "silver"},
)
dp.create_auto_cdc_flow(
    target="silver_order_items",
    source="order_item_clean",
    keys=["order_id","line_no"],
    sequence_by="updated_at",
    except_column_list=["ingested_At"],
    stored_as_scd_type=1
)