from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="bronze.sensor_events_raw", comment="Raw sensor events from Event Hub.")
@dp.expect_or_drop("valid_payload", "payload IS NOT NULL")
def sensor_events_raw():
    conn_str = dbutils.secrets.get(scope="pulsestream", key="eventhub-conn-str")
    kafka_options = {
        "kafka.bootstrap.servers": "pulsestream-eh-prod.servicebus.windows.net:9093",
        "subscribe": "sensor-events",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config":
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="$ConnectionString" password="{conn_str}";',
        "startingOffsets": "earliest",
    }
    return (
        spark.readStream.format("kafka").options(**kafka_options).load()
        .selectExpr("CAST(value AS STRING) as payload", "timestamp as kafka_ts")
        .withColumn("_ingested_at", F.current_timestamp())
    )