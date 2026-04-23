# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Silver Layer — Rides One Big Table (OBT) Construction
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook constructs the **Silver OBT (One Big Table)** for the ride-booking pipeline, producing a fully enriched, denormalised rides table used as the foundation for downstream Gold layer dimension and fact tables.
# MAGIC
# MAGIC ### Pipeline Position
# MAGIC `Bronze (raw JSON) → Bronze Staging (metadata-driven stream) → Silver OBT → Gold (Star Schema)`
# MAGIC
# MAGIC ### What this notebook does
# MAGIC 1. Defines an explicit PySpark schema for the rides JSON payload
# MAGIC 2. Parses and flattens the raw JSON column from `uber.bronze.rides_raw`
# MAGIC 3. Verifies the staging table produced by the metadata-driven streaming pipeline
# MAGIC 4. Constructs the OBT SQL query dynamically using a **Jinja2 template**, joining staging rides against all reference/mapping tables
# MAGIC 5. Profiles each future Gold dimension from the Silver OBT
# MAGIC 6. Validates the Gold layer join pattern against the SCD Type 2 `__END_AT` filter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1 — Environment Setup & Schema Definition

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reset Staging Table
# MAGIC Drops the staging table before re-running the pipeline to ensure a clean state.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE uber.bronze.stg_rides

# COMMAND ----------

# PySpark type and function imports required for schema definition and JSON parsing
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Rides Schema Definition
# MAGIC
# MAGIC An explicit `StructType` schema is defined for the rides JSON payload.
# MAGIC Enforcing the schema at read time ensures type safety and avoids schema inference overhead on large streaming volumes.

# COMMAND ----------

rides_schema = StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2 — Bronze Raw Parsing: JSON to Structured DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC #### Optional: Inspect Raw Bronze Table
# MAGIC Use to verify raw `rides_raw` records before parsing. Retained for diagnostic reference.

# COMMAND ----------

# df = spark.read.table("uber.bronze.rides_raw")
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema Reference
# MAGIC The rides schema above was derived from the `bulk_rides` bronze table.
# MAGIC The `df.schema` output is preserved inline above for transparency and reproducibility.

# COMMAND ----------

# Schema was derived from uber.bronze.bulk_rides; retained here for reference
# df = spark.sql("select * from uber.bronze.bulk_rides")
# rides_schema = df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 — Parse JSON Column into Nested Struct
# MAGIC
# MAGIC The raw `rides` column contains a JSON string. `from_json` is applied against the defined schema,
# MAGIC producing a new `parsed_rides` struct column containing all typed ride fields.

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")

# Parses the raw JSON 'rides' column into a typed struct using the defined schema
df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 — Validate Parsing with Single-Field Selection
# MAGIC
# MAGIC Selects a single field (`ride_id`) from the parsed struct to confirm the JSON-to-struct
# MAGIC transformation succeeded before performing a full field expansion.

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")

# Single-field selection to validate successful struct parsing
df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
              .select("parsed_rides.ride_id")

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 — Flatten Struct: Select All Parsed Fields
# MAGIC
# MAGIC Expands all fields from the parsed struct using wildcard selection (`parsed_rides.*`),
# MAGIC producing a fully flattened DataFrame equivalent to the original rides schema.

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")

# Wildcard selection flattens the parsed struct into individual typed columns
df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
              .select("parsed_rides.*")

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3 — Staging Layer: Metadata-Driven Streaming Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify Staging Table Before Pipeline Reset

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.stg_rides

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reset: Drop Staging Table Prior to Pipeline Re-run
# MAGIC The staging table is dropped to allow the metadata-driven PySpark Streaming pipeline
# MAGIC to repopulate it from scratch on the next run.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE uber.bronze.stg_rides

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: Confirm Staging Table Repopulated by Streaming Pipeline
# MAGIC Run after the metadata-driven pipeline completes to confirm `stg_rides` has been
# MAGIC successfully repopulated with streaming data.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.stg_rides

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4 — Jinja2-Templated OBT Query Construction

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The Silver OBT is assembled by joining the staging rides table against all reference/mapping
# MAGIC tables (vehicle types, vehicle makes, ride statuses, payment methods, cities, cancellation reasons).
# MAGIC
# MAGIC Rather than hardcoding a monolithic SQL JOIN statement, the query is generated dynamically
# MAGIC using a **Jinja2 template** driven by a configuration list. This approach makes the OBT query
# MAGIC maintainable and extensible — adding a new dimension requires only a new config entry.

# COMMAND ----------

pip install Jinja2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Jinja2 Configuration
# MAGIC
# MAGIC Each entry in `jinja_config` defines one table contributing to the OBT:
# MAGIC - `table`: the fully qualified table name and alias
# MAGIC - `select`: the columns to project from that table
# MAGIC - `on`: the JOIN predicate (omitted for the base/fact table)
# MAGIC - `where`: optional filter condition (empty string = no filter)
# MAGIC
# MAGIC The first entry (`stg_rides`) acts as the base table; all subsequent entries are rendered as LEFT JOINs.

# COMMAND ----------

jinja_config = [
    {
        "table" : "uber.bronze.stg_rides stg_rides",
        "select" : "stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating",
        "where" : ""
    },

    {
        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where" : "",
        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type, map_vehicle_types.description, map_vehicle_types.base_rate,map_vehicle_types.per_mile, map_vehicle_types.per_minute",
        "where" : "",
        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
      {
        "table" : "uber.bronze.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.ride_status",
        "where" : "",
        "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "uber.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "uber.bronze.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at",
        "where" : "",
        "on" : "stg_rides.pickup_city_id = map_cities.city_id"
    },
    {
        "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Render Jinja2 Template to SQL
# MAGIC
# MAGIC The template iterates over `jinja_config` to dynamically build the SELECT column list,
# MAGIC FROM clause (base table), and LEFT JOIN chain. An optional WHERE clause is rendered
# MAGIC if any config entry provides a non-empty `where` value.

# COMMAND ----------

from jinja2 import Template

jinja_string = """

    SELECT
        {% for config in jinja_config %}       
            {{config.select }}
                {% if not loop.last %}
                    ,
                    {% endif %}
        {% endfor %}
    FROM
        {% for config in jinja_config %}
            {% if loop.first %}
                {{config.table}}
            {% else %}
                LEFT JOIN {{ config.table }} ON {{ config.on }}
            {% endif %}
        {% endfor %}

        {% for config in jinja_config %}

            {% if loop.first %}
                {% if config.where != "" %}
                WHERE
                {% endif %}
            {% endif %}        

            {{ config.where }}
                {% if not loop.last %}
                    {% if config.where != "" %}
                    AND
                    {% endif %}
                {% endif %}
        {% endfor %}
"""

template = Template(jinja_string)
rendered_template = template.render(jinja_config=jinja_config)

print(rendered_template)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Execute Rendered OBT Query
# MAGIC Renders the Jinja2 template and executes the resulting SQL against the Spark session,
# MAGIC returning the fully joined Silver OBT.

# COMMAND ----------

template = Template(jinja_string)
rendered_template = template.render(jinja_config=jinja_config)
display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %md
# MAGIC #### OBT Query Output Validation
# MAGIC Re-executes the rendered OBT query to inspect the full output and verify
# MAGIC column completeness and join correctness across all dimension tables.

# COMMAND ----------

display(spark.sql(rendered_template))


# COMMAND ----------

# MAGIC %md
# MAGIC #### Manual Join Verification — Vehicle Dimensions
# MAGIC Equivalent SQL query joining `stg_rides` to `map_vehicle_types` and `map_vehicle_makes`
# MAGIC directly, used to cross-validate the Jinja2-rendered output against a hand-written reference query.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   stg_rides.*
# MAGIC FROM
# MAGIC   uber.bronze.stg_rides stg_rides
# MAGIC LEFT JOIN
# MAGIC   uber.bronze.map_vehicle_types map_vehicle_types
# MAGIC ON
# MAGIC   stg_rides.vehicle_type_id= map_vehicle_types.vehicle_type_id
# MAGIC LEFT JOIN
# MAGIC   uber.bronze.map_vehicle_makes map_vehicle_makes
# MAGIC ON
# MAGIC   stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
# MAGIC        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5 — Silver OBT: Dimension Profiling

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inspect Pipeline Execution Timestamp
# MAGIC Captures the current timestamp as a reference point for pipeline run timing.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select current_timestamp()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inspect Silver OBT — Full Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM uber.bronze.silver_obt 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Passenger Dimension
# MAGIC Selects distinct passenger records from the Silver OBT, deduplicating on `passenger_id`.
# MAGIC These records form the basis of `dim_passenger` in the Gold layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   DISTINCT(passenger_id) as passenger_id,
# MAGIC     passenger_name,
# MAGIC     passenger_email,
# MAGIC     passenger_phone
# MAGIC FROM
# MAGIC     uber.bronze.silver_obt   

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     passenger_name,
# MAGIC     passenger_email,
# MAGIC     passenger_phone,
# MAGIC     passenger_id
# MAGIC FROM
# MAGIC     uber.bronze.silver_obt    

# COMMAND ----------

# Deduplicates passenger records on passenger_id to produce the candidate dim_passenger set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone")
    df = df.dropDuplicates(subset=['passenger_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: dim_passenger Gold Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.dim_passenger

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Driver Dimension
# MAGIC Deduplicates driver records on `driver_id` to produce the candidate `dim_driver` set.

# COMMAND ----------

# Deduplicates driver records on driver_id to produce the candidate dim_driver set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license")
    df = df.dropDuplicates(subset=['driver_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Vehicle Dimension
# MAGIC Deduplicates vehicle records on `vehicle_id`, including resolved make and type descriptors
# MAGIC from the OBT joins.

# COMMAND ----------

# Deduplicates vehicle records on vehicle_id to produce the candidate dim_vehicle set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("vehicle_id", "vehicle_make_id", "vehicle_type_id", "vehicle_model","vehicle_color", "license_plate", "vehicle_make", "vehicle_type")
    df = df.dropDuplicates(subset=['vehicle_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Payment Method Dimension
# MAGIC Deduplicates payment method records on `payment_method_id`.

# COMMAND ----------

# Deduplicates payment method records on payment_method_id to produce the candidate dim_payment_method set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth")
    df = df.dropDuplicates(subset=['payment_method_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Booking/Ride Dimension
# MAGIC Deduplicates ride records on `ride_id`, capturing booking metadata, dropoff details,
# MAGIC and ride status for `dim_booking`.

# COMMAND ----------

# Deduplicates ride records on ride_id to produce the candidate dim_booking set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("ride_id", "confirmation_number", "dropoff_Location_id", "ride_status_id","dropoff_city_id", "cancellation_reason_id", "dropoff_address","dropoff_latitude", "dropoff_longitude", "booking_timestamp", "dropoff_timestamp")
    df = df.dropDuplicates(subset=['ride_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Location Dimension
# MAGIC Deduplicates city records on `pickup_city_id` to produce the candidate `dim_location` set.

# COMMAND ----------

# Deduplicates location records on pickup_city_id to produce the candidate dim_location set
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("pickup_city_id", "pickup_city", "region","state","city_updated_at")
    df = df.dropDuplicates(subset=['pickup_city_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify: dim_booking and dim_location Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.dim_booking

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.dim_location

# COMMAND ----------

# MAGIC %md
# MAGIC #### Profile: Fare / Metrics Set
# MAGIC Projects fare-related and trip metric fields from the Silver OBT.
# MAGIC These fields contribute to the `fact_rides` table in the Gold layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM uber.bronze.silver_obt

# COMMAND ----------

# Selects fare and trip metric fields that will populate the Gold fact table
    df = spark.read.table("uber.bronze.silver_obt")
    df = df.select("distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_mutiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute")
    df = df.dropDuplicates(subset=['pickup_city_id'])
    display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Inspect Gold Fact Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uber.bronze.fact

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6 — Gold Layer Validation: SCD Type 2 Join Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC #### Anti-Pattern: JOIN Without SCD Filter Produces Duplicate Records
# MAGIC
# MAGIC Joining `fact` to `dim_location` on `pickup_city_id` alone — without filtering on the active
# MAGIC SCD Type 2 record — returns one row per historical version of each location, causing fan-out duplicates.
# MAGIC This query is retained to illustrate the incorrect pattern; **do not use this in production**.

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id, fact.base_fare, dim.region from uber.bronze.fact as fact
# MAGIC LEFT JOIN uber.bronze.dim_location as dim
# MAGIC ON
# MAGIC     fact.pickup_city_id = dim.pickup_city_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Correct Pattern: Filtering on Active SCD Type 2 Records

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SCD Type 2 Join — Filter on `__END_AT IS NULL`
# MAGIC
# MAGIC To avoid duplicate rows when joining against an SCD Type 2 dimension, the join predicate
# MAGIC must include `AND dim.__END_AT IS NULL`. This condition restricts the join to the currently
# MAGIC active record for each dimension key, ensuring a 1:1 relationship between fact and dimension rows.

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id, fact.base_fare, dim.region from uber.bronze.fact as fact
# MAGIC LEFT JOIN uber.bronze.dim_location as dim
# MAGIC ON
# MAGIC     fact.pickup_city_id = dim.pickup_city_id
# MAGIC     AND dim.__END_AT IS NULL

# COMMAND ----------