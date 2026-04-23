"""
Silver Layer — Unified Staging Table: Bulk + Real-Time Ride Ingestion
Pipeline Definition File: Spark Declarative Pipelines

Constructs the unified Silver staging table `stg_rides` by merging two data sources
into a single streaming Delta table using the `@dp.append_flow` pattern:

  1. rides_bulk  — Historical backfill from `bulk_rides` (Bronze batch/static load)
  2. rides_stream — Real-time events from `rides_raw` (Bronze Kafka/Event Hubs stream)

Both flows target the same `stg_rides` table. The append-flow pattern allows multiple
independent sources — with different schemas and loading frequencies — to be unified
into one consistent staging surface without reprocessing already-ingested data.

`stg_rides` is the primary input for the Silver OBT and all downstream Gold layer tables.

Pipeline position:
    bulk_rides (Bronze) ──┐
                          ├──► stg_rides (Silver staging) ──► Silver OBT ──► Gold Star Schema
    rides_raw  (Bronze) ──┘
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ---------------------------------------------------------------------------
# Rides schema definition
# Explicit schema for the rides JSON payload consumed from `rides_raw`.
# Enforcing the schema at parse time ensures type safety and avoids schema
# inference overhead on high-volume streaming data.
# ---------------------------------------------------------------------------

rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])


# ---------------------------------------------------------------------------
# Unified staging target: stg_rides
# A single streaming Delta table that receives appended records from both
# the bulk historical load and the real-time Kafka stream.
# ---------------------------------------------------------------------------

dp.create_streaming_table("stg_rides")


# ---------------------------------------------------------------------------
# Flow 1: Bulk / Historical Load
# Reads from `bulk_rides` — the Bronze table containing the initial historical
# dataset loaded from Azure Blob Storage. booking_timestamp is explicitly cast
# to TimestampType here because the bulk source stores it as a string, unlike
# the streaming path where the type is enforced via rides_schema at parse time.
# ---------------------------------------------------------------------------

@dp.append_flow(
    target = "stg_rides")

def rides_bulk():
    df = spark.readStream.table("bulk_rides")
    df = df.withColumn("booking_timestamp", col("booking_timestamp").cast("timestamp"))
    return df


# ---------------------------------------------------------------------------
# Flow 2: Real-Time Streaming Load
# Reads from `rides_raw` — the Bronze streaming table populated by the Kafka
# Event Hubs consumer. The raw `rides` column contains a JSON string payload
# which is parsed and flattened using rides_schema before appending to stg_rides.
# ---------------------------------------------------------------------------

@dp.append_flow(
    target = "stg_rides")

def rides_stream():
    df = spark.readStream.table("rides_raw")
    df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
                .select("parsed_rides.*")
    return df_parsed