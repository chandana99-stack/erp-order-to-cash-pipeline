# Databricks notebook source
# MAGIC %md
# MAGIC # _common · Shared config and helpers
# MAGIC Every pipeline notebook loads this with `%run ./_common`. It reads the widgets, builds table names, and holds the reusable write helpers:
# MAGIC
# MAGIC - `write_batch` – idempotent append (re-running a batch replaces that batch instead of duplicating it)
# MAGIC - `upsert` – Delta Lake MERGE on business keys (insert new rows, update changed rows)
# MAGIC - `scd2_merge` – Slowly Changing Dimension Type 2 (keep full history of changes)
# MAGIC - `overwrite` – full refresh of a table

# COMMAND ----------

from datetime import date
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = dbutils.widgets.get("catalog")
BATCH_DATE = dbutils.widgets.get("batch_date").strip() or date.today().isoformat()
LANDING_PATH = f"/Volumes/{CATALOG}/erp_raw/landing"

ENTITIES = ["customers", "products", "sales_orders", "order_items", "deliveries", "invoices", "payments"]


def tbl(schema, name):
    return f"{CATALOG}.{schema}.{name}"


def table_exists(name):
    return spark.catalog.tableExists(name)


print(f"catalog={CATALOG}  batch_date={BATCH_DATE}  landing={LANDING_PATH}")

# COMMAND ----------

def overwrite(df, table):
    """Full refresh: replace the whole table."""
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(table))


def write_batch(df, table, batch_date):
    """Idempotent load: replace only this batch's rows, keep every other batch."""
    if not table_exists(table):
        df.write.format("delta").saveAsTable(table)
        return
    (df.write.format("delta").mode("overwrite")
       .option("replaceWhere", f"_batch_date = '{batch_date}'")
       .saveAsTable(table))


def upsert(df, table, keys):
    """Delta MERGE: update rows whose key already exists, insert the rest."""
    if not table_exists(table):
        df.write.format("delta").saveAsTable(table)
        return
    df.createOrReplaceTempView("upsert_source")
    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING upsert_source AS s
        ON {cond}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def scd2_merge(updates, table, key, tracked_cols, change_date_col):
    """
    SCD Type 2 with a single MERGE.
    - New key            -> insert as current, valid from 1900-01-01
    - Tracked col change -> close the current row (is_current=false, effective_to=change date)
                            and insert a new current row from the change date
    - No change          -> do nothing
    """
    hash_expr = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("~")) for c in tracked_cols]), 256)
    updates = updates.withColumn("row_hash", hash_expr)
    payload_cols = [key] + tracked_cols

    if not table_exists(table):
        first = (updates
            .withColumn("effective_from", F.lit("1900-01-01").cast("date"))
            .withColumn("effective_to", F.lit(None).cast("date"))
            .withColumn("is_current", F.lit(True))
            .withColumn("customer_sk", F.xxhash64(key, "effective_from"))
            .select("customer_sk", *payload_cols, "row_hash", "effective_from", "effective_to", "is_current"))
        first.write.format("delta").saveAsTable(table)
        return

    updates.createOrReplaceTempView("scd2_updates")
    cols_list = ", ".join(payload_cols)
    src_cols = ", ".join(f"src.{c}" for c in payload_cols)
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING (
            -- every incoming row, keyed: closes the old version (or inserts a brand-new key)
            SELECT u.{key} AS merge_key, u.* FROM scd2_updates u
            UNION ALL
            -- changed rows again with a NULL key: forces the insert of the new version
            SELECT NULL AS merge_key, u.*
            FROM scd2_updates u
            JOIN {table} d ON d.{key} = u.{key} AND d.is_current AND d.row_hash <> u.row_hash
        ) AS src
        ON t.{key} = src.merge_key AND t.is_current
        WHEN MATCHED AND t.row_hash <> src.row_hash THEN UPDATE SET
            is_current = false,
            effective_to = src.{change_date_col}
        WHEN NOT MATCHED THEN INSERT (customer_sk, {cols_list}, row_hash, effective_from, effective_to, is_current)
        VALUES (
            xxhash64(src.{key}, CASE WHEN src.merge_key IS NULL THEN src.{change_date_col} ELSE DATE'1900-01-01' END),
            {src_cols}, src.row_hash,
            CASE WHEN src.merge_key IS NULL THEN src.{change_date_col} ELSE DATE'1900-01-01' END,
            NULL, true
        )
    """)

# COMMAND ----------

def try_date(col_name):
    """Parse dates that arrive in either yyyy-MM-dd or MM/dd/yyyy; bad values become NULL instead of failing."""
    return F.coalesce(
        F.to_date(F.expr(f"try_to_timestamp(trim({col_name}), 'yyyy-MM-dd')")),
        F.to_date(F.expr(f"try_to_timestamp(trim({col_name}), 'MM/dd/yyyy')")),
    )


def try_num(col_name, sql_type):
    return F.expr(f"try_cast(trim({col_name}) AS {sql_type})")


def latest_per_key(df, keys, order_col="_ingested_at"):
    """De-duplicate: keep one row per key (the most recently ingested)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
