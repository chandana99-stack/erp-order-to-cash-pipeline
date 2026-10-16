# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: clean, type, de-duplicate, MERGE
# MAGIC For each entity in this batch:
# MAGIC 1. **Clean & type** – trim spaces, fix casing, blank → NULL, parse two date formats, cast numbers safely (`try_cast`).
# MAGIC 2. **De-duplicate** – one row per business key (latest ingested wins).
# MAGIC 3. **Validate** – business rules + referential integrity (e.g. an order's customer must exist). Bad rows go to `erp_silver.quarantine` with the reason, instead of silently disappearing.
# MAGIC 4. **MERGE** good rows into `erp_silver.<entity>` (update existing keys, insert new ones).
# MAGIC
# MAGIC Entities run parent-first (customers → products → orders → items → deliveries → invoices → payments) so integrity checks see this batch's parents.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_date", "", "Batch date (blank = today)")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

def clean_text(c):
    """trim + collapse inner spaces; empty string -> NULL"""
    t = F.regexp_replace(F.trim(F.col(c)), r"\s+", " ")
    return F.when(t == "", None).otherwise(t)


AUDIT = ["_ingested_at", "_batch_date"]

CLEANERS = {
    "customers": lambda df: df.select(
        F.upper(clean_text("customer_id")).alias("customer_id"),
        clean_text("customer_name").alias("customer_name"),
        F.lower(clean_text("email")).alias("email"),
        F.initcap(clean_text("city")).alias("city"),
        F.upper(clean_text("state")).alias("state"),
        F.initcap(clean_text("segment")).alias("segment"),
        try_num("credit_limit", "DECIMAL(12,2)").alias("credit_limit"),
        try_date("updated_at").alias("updated_at"),
        *AUDIT),
    "products": lambda df: df.select(
        F.upper(clean_text("product_id")).alias("product_id"),
        clean_text("product_name").alias("product_name"),
        F.initcap(clean_text("category")).alias("category"),
        try_num("unit_price", "DECIMAL(10,2)").alias("unit_price"),
        *AUDIT),
    "sales_orders": lambda df: df.select(
        F.upper(clean_text("order_id")).alias("order_id"),
        F.upper(clean_text("customer_id")).alias("customer_id"),
        try_date("order_date").alias("order_date"),
        F.initcap(clean_text("sales_rep")).alias("sales_rep"),
        F.upper(clean_text("status")).alias("status"),
        *AUDIT),
    "order_items": lambda df: df.select(
        F.upper(clean_text("order_id")).alias("order_id"),
        try_num("line_no", "INT").alias("line_no"),
        F.upper(clean_text("product_id")).alias("product_id"),
        try_num("quantity", "INT").alias("quantity"),
        try_num("unit_price", "DECIMAL(10,2)").alias("unit_price"),
        *AUDIT).withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("DECIMAL(14,2)")),
    "deliveries": lambda df: df.select(
        F.upper(clean_text("delivery_id")).alias("delivery_id"),
        F.upper(clean_text("order_id")).alias("order_id"),
        try_date("ship_date").alias("ship_date"),
        try_date("delivered_date").alias("delivered_date"),
        clean_text("carrier").alias("carrier"),
        *AUDIT),
    "invoices": lambda df: df.select(
        F.upper(clean_text("invoice_id")).alias("invoice_id"),
        F.upper(clean_text("order_id")).alias("order_id"),
        try_date("invoice_date").alias("invoice_date"),
        try_date("due_date").alias("due_date"),
        try_num("amount", "DECIMAL(12,2)").alias("amount"),
        F.upper(clean_text("currency")).alias("currency"),
        *AUDIT),
    "payments": lambda df: df.select(
        F.upper(clean_text("payment_id")).alias("payment_id"),
        F.upper(clean_text("invoice_id")).alias("invoice_id"),
        try_date("payment_date").alias("payment_date"),
        try_num("amount", "DECIMAL(12,2)").alias("amount"),
        F.upper(clean_text("method")).alias("method"),
        *AUDIT),
}

KEYS = {"customers": ["customer_id"], "products": ["product_id"], "sales_orders": ["order_id"],
        "order_items": ["order_id", "line_no"], "deliveries": ["delivery_id"],
        "invoices": ["invoice_id"], "payments": ["payment_id"]}

# rule name -> SQL condition that is TRUE when the row is BAD
RULES = {
    "customers":    {"missing updated_at": "updated_at IS NULL"},
    "products":     {"invalid unit_price": "unit_price IS NULL OR unit_price <= 0"},
    "sales_orders": {"invalid order_date": "order_date IS NULL",
                     "unknown status": "status NOT IN ('OPEN','SHIPPED','CANCELLED') OR status IS NULL"},
    "order_items":  {"non-positive quantity": "quantity IS NULL OR quantity <= 0",
                     "invalid unit_price": "unit_price IS NULL OR unit_price <= 0"},
    "deliveries":   {"missing ship_date": "ship_date IS NULL"},
    "invoices":     {"invalid invoice/due date": "invoice_date IS NULL OR due_date IS NULL",
                     "non-positive amount": "amount IS NULL OR amount <= 0"},
    "payments":     {"invalid payment_date": "payment_date IS NULL",
                     "non-positive amount": "amount IS NULL OR amount <= 0"},
}

# child column -> (parent silver table, parent key): referential integrity
PARENTS = {
    "sales_orders": [("customer_id", "customers", "customer_id")],
    "order_items":  [("order_id", "sales_orders", "order_id"), ("product_id", "products", "product_id")],
    "deliveries":   [("order_id", "sales_orders", "order_id")],
    "invoices":     [("order_id", "sales_orders", "order_id")],
    "payments":     [("invoice_id", "invoices", "invoice_id")],
}

# COMMAND ----------

def validate(entity, df):
    """Adds _reject_reason: '' for good rows, '; '-joined reasons for bad rows."""
    reasons = [F.when(F.col(k).isNull(), F.lit(f"missing {k}")) for k in KEYS[entity]]
    reasons += [F.when(F.expr(cond), F.lit(name)) for name, cond in RULES.get(entity, {}).items()]

    for i, (col, parent, pkey) in enumerate(PARENTS.get(entity, [])):
        flag = f"_has_parent_{i}"
        parent_keys = spark.table(tbl("erp_silver", parent)).select(F.col(pkey).alias(f"_pk_{i}")).distinct()
        df = (df.join(parent_keys, df[col] == parent_keys[f"_pk_{i}"], "left")
                .withColumn(flag, F.col(f"_pk_{i}").isNotNull()).drop(f"_pk_{i}"))
        reasons.append(F.when(~F.col(flag), F.lit(f"{col} not found in {parent}")))

    df = df.withColumn("_reject_reason", F.concat_ws("; ", *reasons))
    return df.drop(*[c for c in df.columns if c.startswith("_has_parent_")])


rejected_all = []
for entity in ENTITIES:
    bronze = spark.table(tbl("erp_bronze", entity)).filter(F.col("_batch_date") == BATCH_DATE)
    cleaned = latest_per_key(CLEANERS[entity](bronze), KEYS[entity])
    checked = validate(entity, cleaned)

    good = checked.filter("_reject_reason = ''").drop("_reject_reason")
    bad = checked.filter("_reject_reason <> ''")

    upsert(good, tbl("erp_silver", entity), KEYS[entity])

    rejected_all.append(bad.select(
        F.lit(entity).alias("entity"),
        F.concat_ws("|", *[F.coalesce(F.col(k).cast("string"), F.lit("NULL")) for k in KEYS[entity]]).alias("record_key"),
        F.col("_reject_reason").alias("reason"),
        F.to_json(F.struct(*[c for c in bad.columns if not c.startswith("_")])).alias("record_json"),
        "_ingested_at", "_batch_date"))
    print(f"{entity:<13} in={bronze.count():>6}  after_dedupe={cleaned.count():>6}  rejected={bad.count():>4}")

# COMMAND ----------

from functools import reduce
quarantine = reduce(lambda a, b: a.unionByName(b), rejected_all)
write_batch(quarantine, tbl("erp_silver", "quarantine"), BATCH_DATE)
display(spark.table(tbl("erp_silver", "quarantine"))
        .filter(F.col("_batch_date") == BATCH_DATE)
        .groupBy("entity", "reason").count().orderBy("entity"))
