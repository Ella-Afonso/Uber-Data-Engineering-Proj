"""
Bronze Layer — Real-Time Ride Event Ingestion
Pipeline Definition File: Spark Declarative Pipelines

Reads live ride-booking events from Azure Event Hubs using the Kafka protocol
and lands them as a streaming Bronze Delta table (`rides_raw`) via the
Spark Declarative Pipelines (@dp.table) decorator.

Pipeline position:
    Azure Event Hubs (Kafka endpoint) → rides_raw (Bronze Delta) → Silver OBT → Gold Star Schema

Credential pattern:
    The Event Hubs connection string is injected at runtime via Spark pipeline configuration
    (`spark.conf.get`) rather than hardcoded, keeping secrets out of source control.
"""

from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ---------------------------------------------------------------------------
# Event Hubs connection parameters
# ---------------------------------------------------------------------------

EH_NAMESPACE = "EH-uber-project"
EH_NAME  = "eh-uber"

# Connection string retrieved from Spark pipeline configuration at runtime
EH_CONN_STR = spark.conf.get("connection_string")


# ---------------------------------------------------------------------------
# Kafka consumer configuration
# Azure Event Hubs exposes a Kafka-compatible endpoint on port 9093.
# SASL_SSL with PLAIN mechanism is required for Event Hubs authentication.
# ---------------------------------------------------------------------------

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" :  10000,   # Milliseconds before a request is considered timed out
  "kafka.session.timeout.ms" :  10000,   # Milliseconds before the consumer session is dropped
  "maxOffsetsPerTrigger"     :  10000,   # Caps records processed per micro-batch for throughput control
  "failOnDataLoss"           : 'true',   # Raises an error if offsets are no longer available (no silent data loss)
  "startingOffsets"          : 'earliest' # On first run, consume all available history from the topic
}


# ---------------------------------------------------------------------------
# Streaming table definition: rides_raw
# @dp.table registers this function as a Spark Declarative Pipelines streaming
# table. The pipeline runtime manages checkpointing and incremental processing.
# ---------------------------------------------------------------------------

@dp.table
def rides_raw():
    df = spark.readStream.format("kafka")\
                .options(**KAFKA_OPTIONS)\
                .load()

    # Cast binary Kafka value payload to string for downstream JSON parsing
    df = df.withColumn("rides",col("value").cast("string"))

    return df