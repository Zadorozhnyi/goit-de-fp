from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame

from utils import get_spark_session, logger, normalize_text_columns, write_parquet

SOURCE_TABLES = ["athlete_bio", "athlete_event_results"]
BRONZE_DIR = Path("bronze")
SILVER_DIR = Path("silver")


def process_table(spark, table: str) -> None:
    """Read one bronze table, clean and deduplicate, save to silver."""
    source_path = BRONZE_DIR / table
    target_path = SILVER_DIR / table

    # Stage 2: read source tables from the bronze layer.
    raw: DataFrame = spark.read.parquet(str(source_path))
    logger.info("Bronze %s: %d rows", table, raw.count())

    # Stage 2: clean text columns and remove duplicate rows.
    clean = normalize_text_columns(raw).dropDuplicates()
    logger.info("Silver %s: %d rows after dedup", table, clean.count())
    clean.show(20, truncate=False)

    # Stage 2: write output to silver/{table}.
    write_parquet(clean, target_path)


if __name__ == "__main__":
    spark = get_spark_session("goit-de-fp-oza-bronze-to-silver")
    try:
        for source_table in SOURCE_TABLES:
            process_table(spark, source_table)
    finally:
        spark.stop()
