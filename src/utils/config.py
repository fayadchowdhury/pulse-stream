from pyspark.sql import SparkSession

VALID_SENSOR_TYPES = "sensor_type IN ('temperature','vibration','pressure','humidity')"
ANOMALY_THRESHOLD_PCT = 1.2  # 20% increase = anomaly

VIBRATION_ZONE_THRESHOLDS = {"watch": 3.0, "critical": 6.0}  # mm/s