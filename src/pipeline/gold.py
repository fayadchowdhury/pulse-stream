from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.utils.config import ANOMALY_THRESHOLD_PCT


# ---- Per-sensor hourly vibration average — feeds the 48h trend comparison ----
@dp.table(name="gold.vibration_hourly", comment="Hourly average vibration per sensor.")
def vibration_hourly():
    return (
        dp.read_stream("silver.sensor_events")
        .where("sensor_type = 'vibration'")
        .withWatermark("event_ts", "1 hour")
        .groupBy("sensor_id", "facility_id", "region", F.window("event_ts", "1 hour"))
        .agg(F.avg("value").alias("hourly_avg_rms"))
    )


# ---- 48h upward trend detection per sensor ----
@dp.materialized_view(
    name="gold.equipment_health_flags",
    comment="Flags equipment where vibration has trended upward over the last 48 hours."
)
def equipment_health_flags():
    df = dp.read("gold.vibration_hourly")
    recent = (
        df.where(F.col("window.start") >= F.expr("current_timestamp() - INTERVAL 6 HOURS"))
        .groupBy("sensor_id", "facility_id", "region")
        .agg(F.avg("hourly_avg_rms").alias("recent_avg"))
    )
    baseline = (
        df.where(
            (F.col("window.start") < F.expr("current_timestamp() - INTERVAL 42 HOURS")) &
            (F.col("window.start") >= F.expr("current_timestamp() - INTERVAL 48 HOURS"))
        )
        .groupBy("sensor_id", "facility_id", "region")
        .agg(F.avg("hourly_avg_rms").alias("baseline_avg"))
    )
    return (
        recent.join(baseline, ["sensor_id", "facility_id", "region"])
        .withColumn("anomaly_flag", F.col("recent_avg") > F.col("baseline_avg") * ANOMALY_THRESHOLD_PCT)
        .withColumn("pct_change", (F.col("recent_avg") - F.col("baseline_avg")) / F.col("baseline_avg") * 100)
    )


# ---- Regional rollup — cross-site comparison for a facilities manager ----
@dp.materialized_view(name="gold.regional_equipment_health", comment="Cross-site rollup by region.")
def regional_equipment_health():
    df = dp.read("gold.equipment_health_flags")
    return (
        df.groupBy("region")
        .agg(F.count("*").alias("total_monitored"), F.sum(F.col("anomaly_flag").cast("int")).alias("flagged_count"))
        .withColumn("flagged_pct", F.col("flagged_count") / F.col("total_monitored") * 100)
    )


# ---- Facility-level rollup — drill-down below region ----
@dp.materialized_view(name="gold.facility_equipment_health", comment="Per-facility rollup, drill-down from region.")
def facility_equipment_health():
    df = dp.read("gold.equipment_health_flags")
    return (
        df.groupBy("facility_id", "region")
        .agg(F.count("*").alias("total_monitored"), F.sum(F.col("anomaly_flag").cast("int")).alias("flagged_count"))
        .withColumn("flagged_pct", F.col("flagged_count") / F.col("total_monitored") * 100)
    )


# ---- All-sensor-type hourly summary — dashboard trend lines beyond just vibration ----
@dp.table(
    name="gold.sensor_type_hourly_summary",
    comment="Hourly avg/min/max per facility and sensor type, across all four sensor types."
)
def sensor_type_hourly_summary():
    return (
        dp.read_stream("silver.sensor_events")
        .withWatermark("event_ts", "1 hour")
        .groupBy("facility_id", "region", "sensor_type", F.window("event_ts", "1 hour"))
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("reading_count"),
        )
    )


# ---- Live snapshot: latest reading per sensor — status board for the dashboard ----
@dp.materialized_view(
    name="gold.latest_sensor_readings",
    comment="Most recent reading per sensor, for a live equipment status view."
)
def latest_sensor_readings():
    df = dp.read("silver.sensor_events")
    w = Window.partitionBy("sensor_id").orderBy(F.col("event_ts").desc())
    return (
        df.withColumn("rn", F.row_number().over(w))
        .where("rn = 1")
        .drop("rn")
        .select("sensor_id", "facility_id", "region", "sensor_type", "value", "unit", "value_zone", "event_ts")
    )