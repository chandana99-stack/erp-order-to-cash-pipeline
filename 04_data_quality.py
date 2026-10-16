# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Data quality checks (the "quality gate")
# MAGIC Runs checks on the silver tables and logs every result to `erp_silver.dq_results`.
# MAGIC
# MAGIC - **error** checks stop the pipeline (the notebook fails, so the gold step never runs on bad data).
# MAGIC - **warn** checks are logged but don't stop anything.
# MAGIC
# MAGIC Each check is a SQL query that returns the number of **bad** rows. 0 = pass.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_date", "", "Batch date (blank = today)")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

S = lambda name: tbl("erp_silver", name)

CHECKS = [
    # (check name, severity, SQL returning count of bad rows)
    ("customers: unique customer_id", "error",
     f"SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM {S('customers')}"),
    ("orders: unique order_id", "error",
     f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {S('sales_orders')}"),
    ("invoices: unique invoice_id", "error",
     f"SELECT COUNT(*) - COUNT(DISTINCT invoice_id) FROM {S('invoices')}"),
    ("orders: customer exists", "error",
     f"SELECT COUNT(*) FROM {S('sales_orders')} o LEFT ANTI JOIN {S('customers')} c ON o.customer_id = c.customer_id"),
    ("order_items: product exists", "error",
     f"SELECT COUNT(*) FROM {S('order_items')} i LEFT ANTI JOIN {S('products')} p ON i.product_id = p.product_id"),
    ("payments: invoice exists", "error",
     f"SELECT COUNT(*) FROM {S('payments')} p LEFT ANTI JOIN {S('invoices')} i ON p.invoice_id = i.invoice_id"),
    ("order_items: quantity > 0", "error",
     f"SELECT COUNT(*) FROM {S('order_items')} WHERE quantity <= 0"),
    ("invoices: due_date >= invoice_date", "error",
     f"SELECT COUNT(*) FROM {S('invoices')} WHERE due_date < invoice_date"),
    ("payments: not more than invoice amount", "warn",
     f"""SELECT COUNT(*) FROM (
            SELECT i.invoice_id FROM {S('invoices')} i JOIN {S('payments')} p ON i.invoice_id = p.invoice_id
            GROUP BY i.invoice_id, i.amount HAVING SUM(p.amount) > i.amount + 0.01)"""),
    ("customers: email present", "warn",
     f"SELECT COUNT(*) FROM {S('customers')} WHERE email IS NULL"),
    ("freshness: orders loaded for this batch", "error",
     f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {S('sales_orders')} WHERE _batch_date = '{BATCH_DATE}'"),
    ("quarantine rate under 5%", "warn",
     f"""SELECT CASE WHEN q.n > 0.05 * b.n THEN q.n ELSE 0 END FROM
            (SELECT COUNT(*) n FROM {S('quarantine')} WHERE _batch_date = '{BATCH_DATE}') q,
            (SELECT {' + '.join(f"(SELECT COUNT(*) FROM {tbl('erp_bronze', e)} WHERE _batch_date = '{BATCH_DATE}')" for e in ENTITIES)} AS n) b"""),
]

# COMMAND ----------

results = []
for name, severity, sql in CHECKS:
    bad = spark.sql(sql).collect()[0][0] or 0
    results.append((BATCH_DATE, name, severity, int(bad), "PASS" if bad == 0 else "FAIL"))

res_df = (spark.createDataFrame(results, "_batch_date string, check_name string, severity string, bad_rows long, status string")
          .withColumn("checked_at", F.current_timestamp()))
write_batch(res_df, tbl("erp_silver", "dq_results"), BATCH_DATE)
display(res_df)

failed = [r for r in results if r[4] == "FAIL" and r[2] == "error"]
if failed:
    raise Exception(f"Data quality gate failed: {[r[1] for r in failed]}")
print("Quality gate passed ✅")
