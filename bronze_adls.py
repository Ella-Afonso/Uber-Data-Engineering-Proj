# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Bronze Layer — Azure Blob Storage Ingestion (Reference & Bulk Ride Data)
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook ingests raw source data from **Azure Blob Storage (ADLS)** into the
# MAGIC `uber.bronze` Delta catalog layer, forming the entry point of the Medallion Architecture pipeline.
# MAGIC
# MAGIC ### Pipeline Position
# MAGIC `Azure Blob Storage (JSON) → Bronze Delta Tables → Silver OBT → Gold Star Schema`
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. Validates connectivity to the Azure Blob Storage endpoint using a single reference file
# MAGIC 2. Diagnoses HTTP response headers and JSON encoding to confirm the payload is well-formed
# MAGIC 3. Ingests all six reference/mapping JSON files into Bronze Delta tables in a single loop
# MAGIC 4. Ingests the bulk rides JSON file into `uber.bronze.bulk_rides` with an idempotency guard
# MAGIC 5. Verifies the ingested tables with spot-check queries
# MAGIC
# MAGIC > **Note:** The SAS token used for Blob Storage access is embedded inline for development purposes.
# MAGIC > In production, credentials should be stored in Azure Key Vault and referenced via a Databricks secret scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Endpoint Validation: Single-File Connectivity Check
# MAGIC
# MAGIC Reads `map_cities.json` directly from the Azure Blob Storage endpoint using `pandas.read_json`
# MAGIC and converts the result to a PySpark DataFrame. This cell confirms that the SAS-authenticated
# MAGIC URL is reachable and that the JSON payload can be parsed before running the full ingestion loop.

# COMMAND ----------

import pandas as pd

df = pd.read_json("https://staccuber.blob.core.windows.net/raw/ingestion/map_cities.json?sp=r&st=2026-04-15T12:49:07Z&se=2026-04-19T12:49:07Z&spr=https&sv=2025-11-05&sr=c&sig=RTcgZD0V3clij9rJQMppFwocFZ7RV36A0xUvMihAIyA%3D")

df_spark = spark.createDataFrame(df)

display(df_spark)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — HTTP Response Diagnostic: JSON Encoding Investigation
# MAGIC
# MAGIC This cell was introduced to diagnose a `ValueError: Unexpected character found when decoding object value`
# MAGIC raised during the initial `pandas.read_json` call. Rather than assuming the data was malformed,
# MAGIC the raw HTTP response is inspected — checking status code, content-type header, and the first 500
# MAGIC characters of the response body — before attempting a manual `json.loads` parse.
# MAGIC
# MAGIC This approach isolates whether the issue originates from the encoding, the content-type declaration,
# MAGIC or an upstream change to the payload structure.

# COMMAND ----------

import requests
import json

url = "https://staccuber.blob.core.windows.net/raw/ingestion/map_cities.json?sp=r&st=2026-04-15T12:49:07Z&se=2026-04-19T12:49:07Z&spr=https&sv=2025-11-05&sr=c&sig=RTcgZD0V3clij9rJQMppFwocFZ7RV36A0xUvMihAIyA%3D"

response = requests.get(url)

print("STATUS:", response.status_code)
print("CONTENT TYPE:", response.headers.get("content-type"))
print(response.text[:500])

json.loads(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Reference Data Ingestion: Mapping Tables
# MAGIC
# MAGIC Iterates over all six reference/mapping JSON files hosted in Azure Blob Storage and writes each
# MAGIC to a corresponding Bronze Delta table in the `uber.bronze` catalog.
# MAGIC
# MAGIC **Tables ingested:**
# MAGIC - `map_cities` — city, state, and region lookup
# MAGIC - `map_cancellation_reasons` — ride cancellation reason codes
# MAGIC - `map_payment_methods` — payment method types and attributes
# MAGIC - `map_ride_statuses` — ride lifecycle status codes
# MAGIC - `map_vehicle_makes` — vehicle manufacturer lookup
# MAGIC - `map_vehicle_types` — vehicle category, base rates, and per-mile/per-minute pricing
# MAGIC
# MAGIC Each file is read via `pandas.read_json`, converted to a PySpark DataFrame, and written to Delta
# MAGIC with `overwrite` mode and `overwriteSchema` enabled to handle upstream schema changes gracefully.

# COMMAND ----------

import pandas as pd

files = [
{"file":"map_cities"},
{"file":"map_cancellation_reasons"},
{"file":"map_payment_methods"},
{"file":"map_ride_statuses"},
{"file":"map_vehicle_makes"},
{"file":"map_vehicle_types"}
]

for file in files:
    url = f"https://staccuber.blob.core.windows.net/raw/ingestion/{file['file']}.json?sp=r&st=2026-04-15T12:49:07Z&se=2026-04-19T12:49:07Z&spr=https&sv=2025-11-05&sr=c&sig=RTcgZD0V3clij9rJQMppFwocFZ7RV36A0xUvMihAIyA%3D"

    df = pd.read_json(url)

# Convert pandas DataFrame to PySpark DataFrame for Delta write
    df_spark = spark.createDataFrame(df)


# Write to Bronze Delta table with schema overwrite enabled
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .option("overwriteSchema", "true")\
            .saveAsTable(f"uber.bronze.{file['file']}")
        


# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Bulk Rides Ingestion (Idempotent)
# MAGIC
# MAGIC Reads the full `bulk_rides.json` dataset from Azure Blob Storage and writes it to
# MAGIC `uber.bronze.bulk_rides` as a Delta table.
# MAGIC
# MAGIC An idempotency guard (`spark.catalog.tableExists`) ensures the table is written only
# MAGIC on the first execution. Subsequent runs skip the write, preventing unintended overwrites
# MAGIC of the bulk historical dataset.

# COMMAND ----------

  url = "https://staccuber.blob.core.windows.net/raw/ingestion/bulk_rides.json?sp=r&st=2026-04-15T12:49:07Z&se=2026-04-19T12:49:07Z&spr=https&sv=2025-11-05&sr=c&sig=RTcgZD0V3clij9rJQMppFwocFZ7RV36A0xUvMihAIyA%3D"

df = pd.read_json(url)
df_spark = spark.createDataFrame(df)
if not spark.catalog.tableExists("uber.bronze.bulk_rides"):
    df_spark.write.format("delta")\
            .mode("overwrite")\
            .saveAsTable(f"uber.bronze.bulk_rides")
    print("This will not run more than 1 time")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Verification Queries
# MAGIC
# MAGIC Spot-checks to confirm Bronze tables were written correctly and are queryable.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: map_cities Reference Table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM uber.bronze.map_cities

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: bulk_rides Historical Dataset

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM uber.bronze.bulk_rides

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: rides_raw Streaming Table

# COMMAND ----------

# MAGIC  %sql
# MAGIC
# MAGIC SELECT * FROM uber.bronze.rides_raw

# COMMAND ----------