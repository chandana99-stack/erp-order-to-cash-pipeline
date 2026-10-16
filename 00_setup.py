# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Run this **once**. It creates the four schemas (folders for tables) and the volume (folder for raw files).
# MAGIC
# MAGIC | Schema | What lives there |
# MAGIC |---|---|
# MAGIC | `erp_raw` | The `landing` volume where raw CSV files arrive |
# MAGIC | `erp_bronze` | Raw data loaded as-is (all text columns, plus load metadata) |
# MAGIC | `erp_silver` | Cleaned, typed, de-duplicated data + quarantine + data quality results |
# MAGIC | `erp_gold` | Star schema (dimensions + facts) and reporting marts |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

for schema in ["erp_raw", "erp_bronze", "erp_silver", "erp_gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"schema ready: {CATALOG}.{schema}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.erp_raw.landing")
print(f"volume ready: /Volumes/{CATALOG}/erp_raw/landing")
