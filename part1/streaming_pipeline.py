import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    from_json,
    isnan,
    lit,
    regexp_replace,
    struct,
    to_json,
    when,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from configs import jdbc_config, kafka_config, spark_config


def build_spark_session() -> SparkSession:
    packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
            "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.3",
        ]
    )
    return (
        SparkSession.builder.appName("goit-fp-oza-streaming")
        .master(spark_config["master"])
        .config("spark.jars", spark_config["mysql_connector_jar"])
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )


def kafka_auth_options(prefix: str = "kafka.") -> dict:
    return {
        f"{prefix}security.protocol": kafka_config["security_protocol"],
        f"{prefix}sasl.mechanism": kafka_config["sasl_mechanism"],
        f"{prefix}sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{kafka_config['username']}' password='{kafka_config['password']}';"
        ),
    }


def write_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    kafka_payload_df = batch_df.select(
        lit(str(uuid.uuid4())).alias("key"),
        to_json(
            struct(
                col("sport"),
                col("medal"),
                col("sex"),
                col("country_noc"),
                col("avg_height"),
                col("avg_weight"),
                col("timestamp"),
            )
        ).alias("value"),
    )

    # Stage 6.a): write data to the output Kafka topic.
    (
        kafka_payload_df.write.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .options(**kafka_auth_options())
        .option("topic", kafka_config["output_topic"])
        .save()
    )

    # Stage 6.b): write data to the MySQL table.
    (
        batch_df.write.format("jdbc")
        .option("url", jdbc_config["jdbc_url"])
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", jdbc_config["output_table"])
        .option("user", jdbc_config["jdbc_user"])
        .option("password", jdbc_config["jdbc_password"])
        .mode("append")
        .save()
    )

    print(f"Processed batch_id={batch_id}")


if __name__ == "__main__":
    os.makedirs("checkpoints/part1", exist_ok=True)
    spark = build_spark_session()

    # Stage 1: read athletes' bio data from MySQL.
    df_bio = (
        spark.read.format("jdbc")
        .options(
            url=jdbc_config["jdbc_url"],
            driver="com.mysql.cj.jdbc.Driver",
            dbtable=jdbc_config["bio_table"],
            user=jdbc_config["jdbc_user"],
            password=jdbc_config["jdbc_password"],
        )
        .load()
    )

    # Stage 2: filter out rows with missing or non-numeric height/weight.
    bio_clean = (
        df_bio.withColumn("height", regexp_replace(col("height").cast(StringType()), ",", "."))
        .withColumn("weight", regexp_replace(col("weight").cast(StringType()), ",", "."))
        .withColumn("height_num", col("height").cast(DoubleType()))
        .withColumn("weight_num", col("weight").cast(DoubleType()))
        .filter(
            col("height_num").isNotNull()
            & col("weight_num").isNotNull()
            & (~isnan(col("height_num")))
            & (~isnan(col("weight_num")))
        )
        .select(
            col("athlete_id").cast("long").alias("athlete_id"),
            col("sex"),
            col("height_num").alias("height"),
            col("weight_num").alias("weight"),
        )
    )

    # Stage 3: read events from Kafka and parse JSON into a DataFrame.
    kafka_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .options(**kafka_auth_options())
        .option("subscribe", kafka_config["input_topic"])
        .option("startingOffsets", "earliest")
        .load()
    )

    schema = StructType(
        [
            StructField("edition", StringType(), True),
            StructField("edition_id", StringType(), True),
            StructField("country_noc", StringType(), True),
            StructField("sport", StringType(), True),
            StructField("event", StringType(), True),
            StructField("result_id", StringType(), True),
            StructField("athlete", StringType(), True),
            StructField("athlete_id", StringType(), True),
            StructField("pos", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("isTeamSport", StringType(), True),
        ]
    )

    results_df = (
        kafka_raw.select(from_json(col("value").cast("string"), schema).alias("data"))
        .select("data.*")
        .withColumn("athlete_id", col("athlete_id").cast("long"))
        .withColumn("medal", when(col("medal").isNull(), lit("none")).otherwise(col("medal")))
        .withColumn("event_ts", current_timestamp())
    )

    # Stage 4: join Kafka competition results with bio data by athlete_id.
    joined_df = results_df.join(bio_clean, on="athlete_id", how="inner")

    # Stage 5: compute average metrics by sport, medal, sex, country_noc + timestamp.
    agg_df = (
        joined_df.groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
        .withColumn("timestamp", current_timestamp())
    )

    query = (
        agg_df.writeStream.foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", "checkpoints/part1/main")
        .start()
    )

    query.awaitTermination()
