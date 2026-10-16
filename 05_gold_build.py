# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: star schema + reporting marts
# MAGIC
# MAGIC **Dimensions**
# MAGIC - `dim_date` – calendar table
# MAGIC - `dim_product` – products
# MAGIC - `dim_customer` – **SCD Type 2**: when a customer moves or gets a new credit limit, the old row is closed and a new row is added, so history is kept
# MAGIC
# MAGIC **Facts**
# MAGIC - `fact_sales` – one row per order line, linked to the customer version that was valid **on the order date**
# MAGIC - `fact_invoices` – one row per invoice with paid / open amount and payment status
# MAGIC
# MAGIC **Marts (answers for the business)**
# MAGIC - `mart_revenue_monthly` – revenue by month, segment and category
# MAGIC - `mart_ar_aging` – open invoices bucketed by days past due
# MAGIC - `mart_dso_monthly` – Days Sales Outstanding by month

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_date", "", "Batch date (blank = today)")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

S = lambda name: tbl("erp_silver", name)
G = lambda name: tbl("erp_gold", name)

# COMMAND ----------

# MAGIC %md ### Dimensions

# COMMAND ----------

dim_date = spark.sql(f"""
    WITH bounds AS (SELECT MIN(order_date) AS d0 FROM {S('sales_orders')})
    SELECT
        CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
        d AS date,
        year(d) AS year, quarter(d) AS quarter, month(d) AS month,
        date_format(d, 'MMMM') AS month_name,
        trunc(d, 'MM') AS month_start, last_day(d) AS month_end,
        date_format(d, 'EEEE') AS day_name,
        dayofweek(d) IN (1, 7) AS is_weekend
    FROM bounds
    LATERAL VIEW explode(sequence(trunc(d0, 'YEAR'), date_add(DATE'{BATCH_DATE}', 120), INTERVAL 1 DAY)) days AS d
""")
overwrite(dim_date, G("dim_date"))

dim_product = spark.sql(f"""
    SELECT xxhash64(product_id) AS product_sk, product_id, product_name, category, unit_price
    FROM {S('products')}
""")
overwrite(dim_product, G("dim_product"))

customer_changes = spark.table(S("customers")).filter(F.col("_batch_date") == BATCH_DATE).select(
    "customer_id", "customer_name", "email", "city", "state", "segment", "credit_limit", "updated_at")
scd2_merge(customer_changes, G("dim_customer"), key="customer_id",
           tracked_cols=["customer_name", "email", "city", "state", "segment", "credit_limit"],
           change_date_col="updated_at")

display(spark.sql(f"""
    SELECT COUNT(*) AS total_rows, SUM(CAST(is_current AS INT)) AS current_rows,
           COUNT(*) - SUM(CAST(is_current AS INT)) AS history_rows
    FROM {G('dim_customer')}"""))

# COMMAND ----------

# MAGIC %md ### Facts

# COMMAND ----------

fact_sales = spark.sql(f"""
    SELECT
        i.order_id, i.line_no,
        CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS order_date_key,
        o.order_date,
        c.customer_sk, o.customer_id,
        p.product_sk, i.product_id,
        o.status AS order_status,
        o.sales_rep,
        i.quantity, i.unit_price, i.line_amount
    FROM {S('order_items')} i
    JOIN {S('sales_orders')} o ON i.order_id = o.order_id
    LEFT JOIN {G('dim_customer')} c
           ON o.customer_id = c.customer_id
          AND o.order_date >= c.effective_from
          AND (c.effective_to IS NULL OR o.order_date < c.effective_to)
    LEFT JOIN {G('dim_product')} p ON i.product_id = p.product_id
""")
overwrite(fact_sales, G("fact_sales"))

fact_invoices = spark.sql(f"""
    WITH paid AS (
        SELECT invoice_id, SUM(amount) AS paid_amount, MAX(payment_date) AS last_payment_date
        FROM {S('payments')} GROUP BY invoice_id
    )
    SELECT
        inv.invoice_id, inv.order_id,
        c.customer_sk, o.customer_id,
        CAST(date_format(inv.invoice_date, 'yyyyMMdd') AS INT) AS invoice_date_key,
        inv.invoice_date, inv.due_date,
        inv.amount,
        COALESCE(p.paid_amount, 0) AS paid_amount,
        GREATEST(inv.amount - COALESCE(p.paid_amount, 0), 0) AS open_amount,
        p.last_payment_date,
        CASE WHEN inv.amount - COALESCE(p.paid_amount, 0) <= 0.01 THEN 'PAID'
             WHEN COALESCE(p.paid_amount, 0) > 0 THEN 'PARTIAL'
             ELSE 'OPEN' END AS payment_status,
        CASE WHEN inv.amount - COALESCE(p.paid_amount, 0) <= 0.01
             THEN datediff(p.last_payment_date, inv.invoice_date) END AS days_to_pay
    FROM {S('invoices')} inv
    JOIN {S('sales_orders')} o ON inv.order_id = o.order_id
    LEFT JOIN paid p ON inv.invoice_id = p.invoice_id
    LEFT JOIN {G('dim_customer')} c
           ON o.customer_id = c.customer_id
          AND inv.invoice_date >= c.effective_from
          AND (c.effective_to IS NULL OR inv.invoice_date < c.effective_to)
""")
overwrite(fact_invoices, G("fact_invoices"))

# COMMAND ----------

# MAGIC %md ### Marts

# COMMAND ----------

mart_revenue = spark.sql(f"""
    SELECT d.month_start AS month, c.segment, pr.category,
           COUNT(DISTINCT f.order_id)    AS orders,
           COUNT(DISTINCT f.customer_id) AS customers,
           SUM(f.quantity)               AS units,
           SUM(f.line_amount)            AS revenue
    FROM {G('fact_sales')} f
    JOIN {G('dim_date')} d     ON f.order_date_key = d.date_key
    JOIN {G('dim_customer')} c ON f.customer_sk = c.customer_sk
    JOIN {G('dim_product')} pr ON f.product_sk = pr.product_sk
    WHERE f.order_status <> 'CANCELLED'
    GROUP BY d.month_start, c.segment, pr.category
""")
overwrite(mart_revenue, G("mart_revenue_monthly"))

mart_aging = spark.sql(f"""
    SELECT f.invoice_id, f.customer_id, cur.customer_name, cur.segment,
           f.invoice_date, f.due_date, f.amount, f.open_amount,
           GREATEST(datediff(DATE'{BATCH_DATE}', f.due_date), 0) AS days_past_due,
           CASE WHEN f.due_date >= DATE'{BATCH_DATE}' THEN 'Current'
                WHEN datediff(DATE'{BATCH_DATE}', f.due_date) <= 30 THEN '1-30'
                WHEN datediff(DATE'{BATCH_DATE}', f.due_date) <= 60 THEN '31-60'
                WHEN datediff(DATE'{BATCH_DATE}', f.due_date) <= 90 THEN '61-90'
                ELSE '90+' END AS aging_bucket,
           DATE'{BATCH_DATE}' AS as_of_date
    FROM {G('fact_invoices')} f
    JOIN {G('dim_customer')} cur ON f.customer_id = cur.customer_id AND cur.is_current
    WHERE f.payment_status <> 'PAID'
""")
overwrite(mart_aging, G("mart_ar_aging"))

# DSO = (accounts receivable at month end / revenue invoiced in the month) * days in the month
mart_dso = spark.sql(f"""
    WITH months AS (
        SELECT DISTINCT month_start, LEAST(month_end, DATE'{BATCH_DATE}') AS period_end
        FROM {G('dim_date')}
        WHERE month_start <= DATE'{BATCH_DATE}'
          AND month_start >= (SELECT trunc(MIN(invoice_date), 'MM') FROM {G('fact_invoices')})
    ),
    billed AS (
        SELECT m.month_start,
               SUM(CASE WHEN i.invoice_date BETWEEN m.month_start AND m.period_end THEN i.amount ELSE 0 END) AS revenue,
               SUM(CASE WHEN i.invoice_date <= m.period_end THEN i.amount ELSE 0 END) AS billed_to_date
        FROM months m CROSS JOIN {S('invoices')} i
        GROUP BY m.month_start
    ),
    collected AS (
        SELECT m.month_start,
               SUM(CASE WHEN p.payment_date <= m.period_end THEN p.amount ELSE 0 END) AS collected_to_date
        FROM months m CROSS JOIN {S('payments')} p
        GROUP BY m.month_start
    )
    SELECT m.month_start AS month,
           b.revenue,
           b.billed_to_date - COALESCE(c.collected_to_date, 0) AS ending_ar,
           datediff(m.period_end, m.month_start) + 1 AS days_in_period,
           ROUND((b.billed_to_date - COALESCE(c.collected_to_date, 0)) / NULLIF(b.revenue, 0)
                 * (datediff(m.period_end, m.month_start) + 1), 1) AS dso_days
    FROM months m
    JOIN billed b USING (month_start)
    LEFT JOIN collected c USING (month_start)
    ORDER BY month
""")
overwrite(mart_dso, G("mart_dso_monthly"))

# COMMAND ----------

display(spark.table(G("mart_dso_monthly")))
display(spark.sql(f"SELECT aging_bucket, COUNT(*) AS invoices, ROUND(SUM(open_amount), 2) AS open_amount FROM {G('mart_ar_aging')} GROUP BY aging_bucket ORDER BY aging_bucket"))
