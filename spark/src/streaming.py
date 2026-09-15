import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    row_number,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min


# ============================================================
# CONFIGURACIÓN
# ============================================================

KAFKA = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

JDBC = os.getenv(
    "POSTGRES_JDBC",
    "jdbc:postgresql://postgres:5432/urbanheat"
)

PG_USER = os.getenv(
    "POSTGRES_USER",
    "urbanheat"
)

PG_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "urbanheat"
)

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://minio:9000"
)


# ============================================================
# SCHEMA DEL JSON
# ============================================================

schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("city_id", StringType(), True),
    StructField("timestamp", StringType(), True),

    StructField(
        "location",
        StructType([
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("district", StringType(), True),
            StructField("zone_type", StringType(), True),
        ]),
        True
    ),

    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("surface_temperature", DoubleType(), True),
    StructField("solar_radiation", DoubleType(), True),
    StructField("battery_level", DoubleType(), True),
    StructField("signal_strength", DoubleType(), True),
    StructField("firmware_version", StringType(), True),
])


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("UrbanHeatStreaming")

    # Spark
    .config("spark.sql.shuffle.partitions", "6")

    # Driver
    .config("spark.driver.host", "urbanheat-spark")
    .config("spark.driver.bindAddress", "0.0.0.0")

    # MinIO / S3
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        MINIO_ENDPOINT
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin"
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    )

    # S3A implementation
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# KAFKA
# ============================================================

raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA
    )
    .option(
        "subscribe",
        "iot.raw.measurements"
    )

    # Durante las pruebas queremos poder leer
    # mensajes que ya existen.
    .option(
        "startingOffsets",
        "earliest"
    )

    .option(
        "failOnDataLoss",
        "false"
    )

    .load()
)


# ============================================================
# PARSEO JSON
# ============================================================

parsed = (
    raw
    .select(
        from_json(
            col("value").cast("string"),
            schema
        ).alias("data")
    )
    .select("data.*")

    .withColumn(
        "event_timestamp",
        to_timestamp("timestamp")
    )

    .withColumn(
        "district",
        col("location.district")
    )

    .withColumn(
        "zone_type",
        col("location.zone_type")
    )

    .withColumn(
        "latitude",
        col("location.latitude")
    )

    .withColumn(
        "longitude",
        col("location.longitude")
    )
)


# ============================================================
# VALIDACIÓN
# ============================================================

valid = (
    parsed
    .withColumn(
        "is_valid",

        (
            col("device_id").isNotNull()
            &
            col("city_id").isNotNull()
            &
            col("event_timestamp").isNotNull()
            &
            col("temperature").between(-40, 70)
            &
            col("humidity").between(0, 100)
            &
            col("battery_level").between(0, 100)
            &
            col("solar_radiation").between(0, 1500)
        )
    )
)


# ============================================================
# FOREACH BATCH
# ============================================================

def write_batch(batch_df, batch_id):

    print(
        f"Procesando micro-batch {batch_id}"
    )

    if batch_df.take(1) == []:
        print(
            f"Micro-batch {batch_id} vacío"
        )
        return

    batch_df.cache()

    # ========================================================
    # BRONZE → MINIO
    # ========================================================

    try:

        (
            batch_df.write
            .mode("append")
            .format("parquet")
            .partitionBy("city_id")
            .save(
                "s3a://urbanheat/bronze/measurements"
            )
        )

        print(
            f"Bronze OK - batch {batch_id}"
        )

    except Exception as exc:

        print(
            f"MinIO Bronze Error: {exc}"
        )


    # ========================================================
    # SILVER → MINIO
    # ========================================================

    cleaned = (
        batch_df
        .filter(
            col("is_valid") == True
        )
    )

    if cleaned.take(1) == []:

        print(
            f"Batch {batch_id}: "
            "sin registros válidos"
        )

        batch_df.unpersist()

        return


    try:

        (
            cleaned.write
            .mode("append")
            .format("parquet")
            .partitionBy(
                "city_id",
                "district"
            )
            .save(
                "s3a://urbanheat/silver/measurements"
            )
        )

        print(
            f"Silver OK - batch {batch_id}"
        )

    except Exception as exc:

        print(
            f"MinIO Silver Error: {exc}"
        )


    # ========================================================
    # COMPROBACIÓN DE DEVICES EN POSTGRES
    # ========================================================

    try:

        devices_df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC)
            .option(
                "dbtable",
                "devices"
            )
            .option(
                "user",
                PG_USER
            )
            .option(
                "password",
                PG_PASSWORD
            )
            .option(
                "driver",
                "org.postgresql.Driver"
            )
            .load()
            .select("device_id")
            .distinct()
        )

        cleaned_verified = (
            cleaned.join(
                devices_df,
                "device_id",
                "inner"
            )
        )

    except Exception as exc:

        print(
            f"Postgres FK check failed: {exc}"
        )

        cleaned_verified = cleaned


    if cleaned_verified.take(1) == []:

        print(
            f"Batch {batch_id}: "
            "ningún device válido"
        )

        batch_df.unpersist()

        return


    # ========================================================
    # CONEXIÓN JDBC
    # ========================================================

    conn = (
        spark
        ._sc
        ._gateway
        .jvm
        .java.sql.DriverManager
        .getConnection(
            JDBC,
            PG_USER,
            PG_PASSWORD
        )
    )

    stmt = conn.createStatement()


    # ========================================================
    # ÚLTIMA MEDICIÓN POR DEVICE
    # ========================================================

    try:

        window_spec = (
            Window
            .partitionBy("device_id")
            .orderBy(
                col("event_timestamp").desc()
            )
        )

        latest = (
            cleaned_verified

            .withColumn(
                "rn",
                row_number()
                .over(window_spec)
            )

            .filter(
                col("rn") == 1
            )

            .select(
                "device_id",
                "event_timestamp",
                "temperature",
                "humidity",
                "surface_temperature",
                "solar_radiation",
                "battery_level",

                col("signal_strength")
                .cast("integer")
                .alias("signal_strength")
            )
        )


        (
            latest.write
            .format("jdbc")
            .option("url", JDBC)
            .option(
                "dbtable",
                "temp_latest_measurements"
            )
            .option(
                "user",
                PG_USER
            )
            .option(
                "password",
                PG_PASSWORD
            )
            .option(
                "driver",
                "org.postgresql.Driver"
            )
            .mode("overwrite")
            .option(
                "truncate",
                "true"
            )
            .save()
        )


        upsert_latest = """

            INSERT INTO latest_measurements
            SELECT *
            FROM temp_latest_measurements

            ON CONFLICT (device_id)

            DO UPDATE SET
                event_timestamp = EXCLUDED.event_timestamp,
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                surface_temperature = EXCLUDED.surface_temperature,
                solar_radiation = EXCLUDED.solar_radiation,
                battery_level = EXCLUDED.battery_level,
                signal_strength = EXCLUDED.signal_strength

        """

        stmt.execute(upsert_latest)

        print(
            f"latest_measurements OK - batch {batch_id}"
        )

    except Exception as exc:

        print(
            "PostgreSQL latest_measurements "
            f"UPSERT Error: {exc}"
        )


    # ========================================================
    # MÉTRICAS HORARIAS
    # ========================================================

    try:

        hourly = (
            cleaned_verified

            .groupBy(
                window(
                    "event_timestamp",
                    "1 hour"
                ),
                "district",
                "zone_type"
            )

            .agg(
                avg(
                    "temperature"
                ).alias(
                    "avg_temperature"
                ),

                spark_max(
                    "temperature"
                ).alias(
                    "max_temperature"
                ),

                spark_min(
                    "temperature"
                ).alias(
                    "min_temperature"
                ),

                avg(
                    "humidity"
                ).alias(
                    "avg_humidity"
                ),

                count("*").alias(
                    "measurement_count"
                )
            )

            .select(
                col(
                    "window.start"
                ).alias(
                    "bucket_start"
                ),

                "district",
                "zone_type",

                "avg_temperature",
                "max_temperature",
                "min_temperature",
                "avg_humidity",

                col(
                    "measurement_count"
                ).cast("bigint")
                .alias(
                    "measurement_count"
                )
            )
        )


        (
            hourly.write
            .format("jdbc")
            .option("url", JDBC)
            .option(
                "dbtable",
                "temp_hourly_metrics"
            )
            .option(
                "user",
                PG_USER
            )
            .option(
                "password",
                PG_PASSWORD
            )
            .option(
                "driver",
                "org.postgresql.Driver"
            )
            .mode("overwrite")
            .option(
                "truncate",
                "true"
            )
            .save()
        )


        upsert_hourly = """

            INSERT INTO hourly_metrics (
                bucket_start,
                district,
                zone_type,
                avg_temperature,
                max_temperature,
                min_temperature,
                avg_humidity,
                measurement_count
            )

            SELECT
                bucket_start,
                district,
                zone_type,
                avg_temperature,
                max_temperature,
                min_temperature,
                avg_humidity,
                measurement_count

            FROM temp_hourly_metrics

            ON CONFLICT (
                bucket_start,
                district,
                zone_type
            )

            DO UPDATE SET

                avg_temperature =
                    (hourly_metrics.avg_temperature
                    + EXCLUDED.avg_temperature) / 2,

                max_temperature =
                    GREATEST(
                        hourly_metrics.max_temperature,
                        EXCLUDED.max_temperature
                    ),

                min_temperature =
                    LEAST(
                        hourly_metrics.min_temperature,
                        EXCLUDED.min_temperature
                    ),

                avg_humidity =
                    (hourly_metrics.avg_humidity
                    + EXCLUDED.avg_humidity) / 2,

                measurement_count =
                    hourly_metrics.measurement_count
                    + EXCLUDED.measurement_count

        """

        stmt.execute(upsert_hourly)

        print(
            f"hourly_metrics OK - batch {batch_id}"
        )

    except Exception as exc:

        print(
            "PostgreSQL hourly_metrics "
            f"UPSERT Error: {exc}"
        )


    # ========================================================
    # LIMPIEZA
    # ========================================================

    stmt.close()
    conn.close()

    batch_df.unpersist()

    print(
        f"Micro-batch {batch_id} finalizado"
    )


# ============================================================
# STREAMING
# ============================================================

query = (
    valid

    .withWatermark(
        "event_timestamp",
        "10 minutes"
    )

    .writeStream

    .foreachBatch(
        write_batch
    )

    .option(
        "checkpointLocation",
        "s3a://urbanheat/checkpoints/streaming"
    )

    .trigger(
        processingTime="10 seconds"
    )

    .start()
)


query.awaitTermination()