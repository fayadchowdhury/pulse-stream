from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

SENSOR_EVENT_SCHEMA = (
    StructType()
    .add("sensor_id", StringType())
    .add("facility_id", StringType())
    .add("region", StringType())
    .add("sensor_type", StringType())
    .add("value", DoubleType())
    .add("unit", StringType())
    .add("event_ts", TimestampType())
)