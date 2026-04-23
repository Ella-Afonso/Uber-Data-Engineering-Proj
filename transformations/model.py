"""
Gold Layer — Star Schema: Streaming Dimension & Fact Table Definitions
Pipeline Definition File: Spark Declarative Pipelines

Defines all Gold layer tables for the ride-booking pipeline using Spark Declarative
Pipelines auto-CDC flows. Each dimension and the fact table is constructed from the
Silver OBT (`uber.bronze.silver_obt`) via a two-step pattern:

  1. A `@dp.view` (or `@dp.table`) projects and deduplicates the relevant columns
     from the Silver OBT streaming table.
  2. `dp.create_streaming_table` + `dp.create_auto_cdc_flow` materialise the view
     as a managed streaming Delta table with change-data-capture applied.

Star Schema produced:
  Dimensions (SCD Type 1 — upsert on natural key):
    dim_passenger, dim_driver, dim_vehicle, dim_payment, dim_booking

  Dimension (SCD Type 2 — full history tracked via city_updated_at):
    dim_location

  Fact:
    fact (grain: one row per unique ride + dimension key combination)

Pipeline position:
    Silver OBT (uber.bronze.silver_obt) → Gold Star Schema (Declarative Pipeline)
"""

from pyspark import pipelines as dp


# ---------------------------------------------------------------------------
# dim_passenger — SCD Type 1
# Unique passengers, deduped on passenger_id.
# Updates in place; no history retained.
# ---------------------------------------------------------------------------

@dp.view
def dim_passenger_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
    df = df.dropDuplicates(subset=['passenger_id'])
    return df

dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
  target = "dim_passenger",
  source = "dim_passenger_view",
  keys = ["passenger_id"],
  sequence_by = "passenger_id",
  stored_as_scd_type = 1,
)


# ---------------------------------------------------------------------------
# dim_driver — SCD Type 1
# Unique drivers, deduped on driver_id.
# Updates in place; no history retained.
# ---------------------------------------------------------------------------

@dp.view
def dim_driver_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
    df = df.dropDuplicates(subset=['driver_id'])
    return df

dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
  target = "dim_driver",
  source = "dim_driver_view",
  keys = ["driver_id"],
  sequence_by = "driver_id",
  stored_as_scd_type = 1,
)  


# ---------------------------------------------------------------------------
# dim_vehicle — SCD Type 1
# Unique vehicles with resolved make and type descriptors, deduped on vehicle_id.
# Updates in place; no history retained.
# ---------------------------------------------------------------------------

@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("vehicle_id", "vehicle_make_id", "vehicle_type_id", "vehicle_model","vehicle_color", "license_plate", "vehicle_make", "vehicle_type")
    df = df.dropDuplicates(subset=['vehicle_id'])
    return df

dp.create_streaming_table("dim_vehicle")
dp.create_auto_cdc_flow(
  target = "dim_vehicle",
  source = "dim_vehicle_view",
  keys = ["vehicle_id"],
  sequence_by = "vehicle_id",
  stored_as_scd_type = 1,
)    


# ---------------------------------------------------------------------------
# dim_payment — SCD Type 1
# Unique payment methods with card and auth attributes, deduped on payment_method_id.
# Updates in place; no history retained.
# ---------------------------------------------------------------------------

@dp.view
def dim_payment_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
    df = df.dropDuplicates(subset=['payment_method_id'])
    return df

dp.create_streaming_table("dim_payment")
dp.create_auto_cdc_flow(
  target = "dim_payment",
  source = "dim_payment_view",
  keys = ["payment_method_id"],
  sequence_by = "payment_method_id",
  stored_as_scd_type = 1,
)    


# ---------------------------------------------------------------------------
# dim_booking — SCD Type 1
# Ride booking records including pickup/dropoff metadata, deduped on ride_id.
# Updates in place; no history retained.
# ---------------------------------------------------------------------------

@dp.view
def dim_booking_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("ride_id", "confirmation_number", "dropoff_Location_id", "ride_status_id","dropoff_city_id", "cancellation_reason_id", "dropoff_address","dropoff_latitude", "dropoff_longitude", "booking_timestamp", "dropoff_timestamp", "pickup_address", "pickup_latitude", "pickup_longitude", "pickup_location_id")
    df = df.dropDuplicates(subset=['ride_id'])
    return df

dp.create_streaming_table("dim_booking")
dp.create_auto_cdc_flow(
  target = "dim_booking",
  source = "dim_booking_view",
  keys = ["ride_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)  


# ---------------------------------------------------------------------------
# dim_location — SCD Type 2
# City/region lookup dimension. Unlike other dimensions, location data is
# versioned: deduplication is applied on (pickup_city_id, city_updated_at)
# and the CDC flow sequences by city_updated_at. This retains the full
# change history per city, enabling point-in-time joins in the Gold layer.
# The active record is identified by __END_AT IS NULL.
# ---------------------------------------------------------------------------

@dp.table
def dim_location_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("pickup_city_id", "pickup_city", "region","state","city_updated_at")
    df = df.dropDuplicates(subset=['pickup_city_id','city_updated_at'])
    return df

dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "city_updated_at",
  stored_as_scd_type = 2,
)  


# ---------------------------------------------------------------------------
# fact — Fact Table (SCD Type 1)
# Central fact table capturing ride-level metrics and foreign keys to all
# dimensions. Grain: one row per unique combination of ride_id and dimension
# surrogate keys. Fare components, trip metrics, and ratings are projected
# directly from the Silver OBT.
# ---------------------------------------------------------------------------

@dp.view
def fact_view():
    df = spark.readStream.table("uber.bronze.silver_obt")
    df = df.select("ride_id","pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id","distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute")
    return df

dp.create_streaming_table("fact")
dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id", "pickup_city_id","payment_method_id","driver_id","passenger_id","vehicle_id"],
  sequence_by = "ride_id",
  stored_as_scd_type = 1,
)