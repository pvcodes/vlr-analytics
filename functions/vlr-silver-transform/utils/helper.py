# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

from pyspark.sql import functions as F


def cast_percentage(col_name: str) -> F.Column:
    """
    "93%" → 0.93
    Strips % symbol, casts to Double, divides by 100.
    NULL input → NULL output.
    """
    return F.regexp_replace(F.col(col_name), "%", "").cast(T.DoubleType()) / F.lit(
        100.0
    )


def cast_ratio_string(col_name: str) -> F.Column:
    """
    "1/2" → 0.5  |  "0/2" → 0.0  |  NULL → NULL
    Splits on "/" and divides numerator by denominator.
    denom = 0 → NULL (guard against division by zero).
    """
    numerator = F.split(F.col(col_name), "/").getItem(0).cast(T.DoubleType())
    denominator = F.split(F.col(col_name), "/").getItem(1).cast(T.DoubleType())

    return (
        F.when(F.col(col_name).isNull(), F.lit(None).cast(T.DoubleType()))
        .when(denominator == 0, F.lit(None).cast(T.DoubleType()))
        .otherwise(F.round(numerator / denominator, 4))
    )


def ratio_mismatch(
    stored_col: str, numerator_col: str, denominator_col: str, tolerance: float
) -> F.Column:
    """
    True if stored VLR ratio deviates > tolerance from raw totals.
    Double nullif guard prevents divide-by-zero on both the raw denominator
    and the computed value itself (e.g. 0 kills / 10 rounds = 0.0).
    """
    denom = F.nullif(F.col(denominator_col), F.lit(0))
    computed = F.col(numerator_col) / denom
    stored = F.col(stored_col)
    safe_computed = F.nullif(computed, F.lit(0.0))
    deviation = F.abs(stored - safe_computed) / F.abs(safe_computed)
    return stored.isNotNull() & safe_computed.isNotNull() & (deviation > tolerance)
