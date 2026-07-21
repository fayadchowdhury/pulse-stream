from pyspark import pipelines as dp
from pyspark.sql import DataFrame, functions as F
from src.utils.schemas import SENSOR_EVENT_SCHEMA
from src.utils.config import VALID_SENSOR_TYPES, VIBRATION_ZONE_THRESHOLDS


def _parse_payload(df: DataFrame) -> DataFrame:
    """Bronze payload string -> typed columns via the sensor event schema."""
    return (
        df.select(F.from_json(F.col("payload"), SENSOR_EVENT_SCHEMA).alias("d"), "_ingested_at", "kafka_ts")
        .select("d.*", "_ingested_at", "kafka_ts")
    )


def _dedupe(df: DataFrame) -> DataFrame:
    """Watermark + dedupe on (sensor_id, event_ts) to drop replayed/duplicate readings."""
    return (
        df.withWatermark("event_ts", "10 minutes")
        .dropDuplicates(["sensor_id", "event_ts"])
    )


def _normalize_ids(df: DataFrame) -> DataFrame:
    """Trim/lowercase IDs so casing drift across producers doesn't fragment groupings."""
    return (
        df.withColumn("sensor_id", F.trim(F.lower(F.col("sensor_id"))))
        .withColumn("facility_id", F.trim(F.lower(F.col("facility_id"))))
    )


def _round_values(df: DataFrame) -> DataFrame:
    """Consistent precision so Gold aggregations aren't noisier than the real signal."""
    return df.withColumn("value", F.round(F.col("value"), 2))


def _add_time_columns(df: DataFrame) -> DataFrame:
    """Derived date/hour columns for partitioning and time-based Gold aggregations."""
    return (
        df.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.date_trunc("hour", "event_ts"))
    )


def _add_ingestion_lag(df: DataFrame) -> DataFrame:
    """Seconds between sensor event time and Kafka arrival time — pipeline staleness signal."""
    return df.withColumn(
        "ingestion_lag_seconds",
        F.col("kafka_ts").cast("long") - F.col("event_ts").cast("long")
    )


def _add_value_zone(df: DataFrame) -> DataFrame:
    """Classify vibration readings into normal/watch/critical zones."""
    watch, critical = VIBRATION_ZONE_THRESHOLDS["watch"], VIBRATION_ZONE_THRESHOLDS["critical"]
    return df.withColumn(
        "value_zone",
        F.when(F.col("sensor_type") == "vibration",
            F.when(F.col("value") < watch, "normal")
            .when(F.col("value") < critical, "watch")
            .otherwise("critical")
        ).otherwise(F.lit("n/a"))
    )


@dp.table(name="silver.sensor_events", comment="Validated, typed, enriched sensor readings.")
@dp.expect_or_drop("has_ids", "sensor_id IS NOT NULL AND facility_id IS NOT NULL")
@dp.expect_or_drop("known_sensor_type", VALID_SENSOR_TYPES)
@dp.expect("plausible_temperature", "sensor_type != 'temperature' OR (value BETWEEN -50 AND 200)")
@dp.expect("plausible_vibration", "sensor_type != 'vibration' OR (value >= 0 AND value < 500)")
@dp.expect("plausible_pressure", "sensor_type != 'pressure' OR (value BETWEEN 0 AND 10000)")
@dp.expect("plausible_humidity", "sensor_type != 'humidity' OR (value BETWEEN 0 AND 100)")
def sensor_events():
    raw = dp.read_stream("bronze.sensor_events_raw")
    return (
        _parse_payload(raw)
        .transform(_dedupe)
        .transform(_normalize_ids)
        .transform(_round_values)
        .transform(_add_time_columns)
        .transform(_add_ingestion_lag)
        .transform(_add_value_zone)
    )