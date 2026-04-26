from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, current_timestamp, regexp_replace

from utils import get_spark_session, logger, write_parquet

SILVER_DIR = Path("silver")
OUTPUT_PATH = Path("gold") / "avg_stats"

GROUP_COLS = ["sport", "medal", "sex", "country_noc"]


def _to_numeric(df: DataFrame, *columns: str) -> DataFrame:
    """Cast comma-or-dot decimal string columns to double, dropping non-parseable rows."""
    result = df
    for column in columns:
        result = result.withColumn(
            column,
            regexp_replace(col(column).cast("string"), ",", ".").cast("double"),
        ).filter(col(column).isNotNull())
    return result


if __name__ == "__main__":
    spark = get_spark_session("goit-de-fp-oza-silver-to-gold")
    try:
        # Stage 3: read silver/athlete_bio and silver/athlete_event_results.
        bio = (
            spark.read.parquet(str(SILVER_DIR / "athlete_bio"))
            .select(
                col("athlete_id").cast("long").alias("athlete_id"),
                "sex", "weight", "height",
            )
        )
        results = (
            spark.read.parquet(str(SILVER_DIR / "athlete_event_results"))
            .select(
                col("athlete_id").cast("long").alias("athlete_id"),
                "country_noc", "sport", "medal",
            )
        )

        # Stage 3: join datasets by athlete_id.
        combined = results.join(bio, on="athlete_id", how="inner")
        numeric = _to_numeric(combined, "height", "weight")

        # Stage 3: compute avg(height) and avg(weight) by sport, medal, sex, country_noc + timestamp.
        aggregated = (
            numeric
            .groupBy(*GROUP_COLS)
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )
        logger.info("Gold rows: %d", aggregated.count())
        aggregated.show(20, truncate=False)

        # Stage 3: write the final dataset to gold/avg_stats.
        write_parquet(aggregated, OUTPUT_PATH)
    finally:
        spark.stop()
