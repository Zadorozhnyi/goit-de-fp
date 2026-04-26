"""
Shared utilities for the multi-hop data lake pipeline (part2).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, when
from pyspark.sql.types import StringType

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("data_lake")


def get_spark_session(app_name: str) -> SparkSession:
    """Return (or create) a SparkSession with the given application name."""
    # In the shared training cluster executors can flap; local mode keeps DAG deterministic.
    return (
        SparkSession.builder
        .master("local[2]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def write_parquet(df: DataFrame, path: Path, mode: str = "overwrite") -> None:
    """Write *df* to *path* in Parquet format."""
    df.write.mode(mode).parquet(str(path))
    logger.info("Saved Parquet output: %s", path)


def normalize_text_columns(df: DataFrame) -> DataFrame:
    """
    Normalize all StringType columns:
    - strip leading/trailing whitespace
    - collapse multiple spaces into one
    - replace NULL-equivalent empty strings with None
    """
    string_cols: List[str] = [
        f.name for f in df.schema.fields if isinstance(f.dataType, StringType)
    ]
    result = df
    for col_name in string_cols:
        result = result.withColumn(
            col_name,
            when(col(col_name).isNull() | (trim(col(col_name)) == ""), None).otherwise(
                trim(regexp_replace(col(col_name), r"\s+", " "))
            ),
        )
    return result
