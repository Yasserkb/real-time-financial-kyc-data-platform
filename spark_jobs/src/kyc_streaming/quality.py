from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class QualitySummary:
    total_rows: int
    invalid_rows: int
    invalid_ratio: float

    @property
    def passed(self) -> bool:
        return self.invalid_ratio <= 0.01


def summarize_quality(df: DataFrame) -> QualitySummary:
    row = df.agg(F.count("*").alias("total_rows"), F.sum(F.when(F.col("is_valid") == "false", F.lit(1)).otherwise(F.lit(0))).alias("invalid_rows")).collect()[0]
    total = int(row["total_rows"] or 0)
    invalid = int(row["invalid_rows"] or 0)
    return QualitySummary(total_rows=total, invalid_rows=invalid, invalid_ratio=invalid / total if total else 0.0)


def assert_quality_gate(df: DataFrame) -> None:
    summary = summarize_quality(df)
    if not summary.passed:
        raise ValueError(f"Quality gate failed: invalid_rows={summary.invalid_rows}, total_rows={summary.total_rows}, invalid_ratio={summary.invalid_ratio:.4f}")
