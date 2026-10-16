# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: load raw files as-is
# MAGIC Reads this batch's CSV files from the landing volume and appends them to `erp_bronze.<entity>`.
# MAGIC
# MAGIC - Every column is kept as **text**, exactly as it arrived (no cleaning here, so nothing is ever lost).
# MAGIC - Three audit columns are added: `_source_file`, `_ingested_at`, `_batch_date`.
# MAGIC - **Idempotent:** re-running the same batch replaces that batch's rows instead of duplicating them (`replaceWhere`).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("batch_date", "", "Batch date (blank = today)")

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

batch_path = f"{LANDING_PATH}/{BATCH_DATE}"

for entity in ENTITIES:
    raw = (spark.read
           .option("header", "true")
           .option("inferSchema", "false")        # bronze keeps everything as text
           .csv(f"{batch_path}/{entity}.csv"))

    bronze = (raw
              .withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_batch_date", F.lit(BATCH_DATE)))

    target = tbl("erp_bronze", entity)
    write_batch(bronze, target, BATCH_DATE)
    print(f"{entity:<13} {bronze.count():>6} rows -> {target}")
