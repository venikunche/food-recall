"""
Silver Layer Transform — PySpark

Reads raw JSON from GCS Bronze layer (both FDA and FSIS),
harmonizes schemas, parses contamination categories,
deduplicates, and writes unified Parquet to GCS Silver layer.

Bronze (GCS JSON) → this job → Silver (GCS Parquet)

Usage:
    spark-submit spark/silver_transform.py \
        --bronze-bucket gs://food-recall-bronze \
        --silver-bucket gs://food-recall-silver \
        --date 2026-04-17
"""

import argparse
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType


# =============================================================================
# CONTAMINATION PARSING RULES
# =============================================================================
# These regex patterns are applied to the reason/recall text to categorize
# the type of contamination. Order matters — first match wins for the
# consolidated contamination_category column.

CONTAMINATION_RULES = {
    "salmonella":              r"(?i)salmonella",
    "listeria":                r"(?i)listeria|l\.\s*monocytogenes",
    "ecoli":                   r"(?i)e\.?\s*coli|escherichia",
    "undeclared_milk":         r"(?i)undeclared.*milk|milk.*undeclared|contains\s+milk",
    "undeclared_peanut":       r"(?i)undeclared.*peanut|peanut.*undeclared",
    "undeclared_soy":          r"(?i)undeclared.*soy|soy.*undeclared",
    "undeclared_egg":          r"(?i)undeclared.*egg|egg.*undeclared",
    "undeclared_wheat":        r"(?i)undeclared.*wheat|wheat.*undeclared|gluten",
    "undeclared_tree_nuts":    r"(?i)undeclared.*(almond|cashew|walnut|pecan|tree.?nut|pistachio|macadamia)",
    "undeclared_allergen_other": r"(?i)undeclared.*allergen|allergen.*undeclared",
    "foreign_object":          r"(?i)foreign (object|material|body|matter)|glass|metal.*fragment|plastic.*fragment|plastic.*piece",
    "mislabeling":             r"(?i)mislabel|misbranded|incorrect.*label|wrong.*label",
}

# Priority order for the consolidated category column
CONTAMINATION_PRIORITY = [
    ("salmonella",              "Salmonella"),
    ("listeria",                "Listeria"),
    ("ecoli",                   "E. coli"),
    ("undeclared_milk",         "Undeclared Allergen - Milk"),
    ("undeclared_peanut",       "Undeclared Allergen - Peanut"),
    ("undeclared_soy",          "Undeclared Allergen - Soy"),
    ("undeclared_egg",          "Undeclared Allergen - Egg"),
    ("undeclared_wheat",        "Undeclared Allergen - Wheat"),
    ("undeclared_tree_nuts",    "Undeclared Allergen - Tree Nuts"),
    ("undeclared_allergen_other", "Undeclared Allergen - Other"),
    ("foreign_object",          "Foreign Object"),
    ("mislabeling",             "Mislabeling"),
]


# =============================================================================
# US STATE NORMALIZATION
# =============================================================================

STATE_NAME_TO_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX",
    "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}


def create_spark_session() -> SparkSession:
    """Create a Spark session configured for GCS access."""
    import os

    # Works in both dev container (/app) and Airflow container (/opt/airflow)
    credentials_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "/app/config/gcp/service-account.json"
    )

    return (
        SparkSession.builder
        .appName("FoodRecall-SilverTransform")
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", credentials_path)
        .master("local[*]")
        .getOrCreate()
    )



# =============================================================================
# NORMALIZE FDA
# =============================================================================

def normalize_fda(df: DataFrame) -> DataFrame:
    """
    Normalize FDA records into the unified schema.

    Handles:
    - Field renaming to unified names
    - Date parsing (report_date is YYYYMMDD string)
    - Adding source_agency column
    """
    return df.select(
        F.col("recall_number").alias("recall_id"),
        F.lit("FDA").alias("source_agency"),
        F.col("classification"),
        F.col("reason_for_recall").alias("reason"),
        F.to_date(F.col("recall_initiation_date"), "yyyyMMdd").alias("recall_date"),
        F.to_date(F.col("report_date"), "yyyyMMdd").alias("report_date"),
        F.col("recalling_firm").alias("company"),
        F.col("state"),
        F.col("city"),
        F.col("product_description"),
        F.col("status"),
        F.col("voluntary_mandated"),
        F.col("distribution_pattern"),
        F.col("product_quantity"),
    )


# =============================================================================
# NORMALIZE FSIS
# =============================================================================

def normalize_fsis(df: DataFrame) -> DataFrame:
    """
    Normalize FSIS records into the unified schema.

    Handles:
    - Field renaming (field_* prefix removal)
    - Date parsing
    - Multi-state field (take first state for the primary state column)
    - Fields not available in FSIS are set to null
    """
    return df.select(
        F.concat(F.lit("FSIS-"), F.monotonically_increasing_id().cast("string")).alias("recall_id"),
        F.lit("FSIS").alias("source_agency"),
        F.col("field_recall_classification").alias("classification"),
        F.col("field_recall_reason").alias("reason"),
        F.to_date(F.col("field_recall_date"), "yyyy-MM-dd").alias("recall_date"),
        F.to_date(F.col("field_recall_date"), "yyyy-MM-dd").alias("report_date"),
        F.col("field_establishment").alias("company"),
        # FSIS states is comma-separated; take the first one
        F.trim(F.split(F.col("field_states"), ",")[0]).alias("state"),
        F.col("field_states").alias("city"),  # no city in FSIS, store full states here
        F.col("field_title").alias("product_description"),
        F.when(F.col("field_closed_date").isNotNull(), "Completed")
         .otherwise("Ongoing").alias("status"),
        F.lit(None).cast(StringType()).alias("voluntary_mandated"),
        F.col("field_states").alias("distribution_pattern"),
        F.lit(None).cast(StringType()).alias("product_quantity"),
    )


# =============================================================================
# SHARED TRANSFORMS
# =============================================================================

def normalize_state(df: DataFrame) -> DataFrame:
    """
    Normalize state values to 2-letter abbreviations.
    Handles: full names, mixed case, extra whitespace.
    Already-abbreviated states (2 chars) are kept as-is.
    """
    mapping_expr = F.create_map(
        *[item for pair in STATE_NAME_TO_ABBREV.items() for item in (F.lit(pair[0]), F.lit(pair[1]))]
    )

    return df.withColumn(
        "state",
        F.when(
            F.length(F.trim(F.col("state"))) == 2,
            F.upper(F.trim(F.col("state")))
        ).otherwise(
            F.coalesce(
                mapping_expr[F.lower(F.trim(F.col("state")))],
                F.upper(F.trim(F.col("state")))
            )
        )
    )


def parse_contamination(df: DataFrame) -> DataFrame:
    """
    Parse the 'reason' field to extract contamination categories.

    Adds:
    - Boolean columns for each contamination type (contam_salmonella, etc.)
    - A consolidated contamination_category column (first match wins)
    """
    # Add boolean flag columns
    for name, pattern in CONTAMINATION_RULES.items():
        df = df.withColumn(
            f"contam_{name}",
            F.when(F.col("reason").rlike(pattern), True).otherwise(False)
        )

    # Build consolidated category using priority order
    category_expr = F.lit("Other")
    # Reverse so highest priority overwrites last
    for col_name, label in reversed(CONTAMINATION_PRIORITY):
        category_expr = F.when(
            F.col(f"contam_{col_name}"), F.lit(label)
        ).otherwise(category_expr)

    df = df.withColumn("contamination_category", category_expr)

    return df


def add_time_dimensions(df: DataFrame) -> DataFrame:
    """Add year, month, quarter columns derived from recall_date."""
    return (
        df
        .withColumn("recall_year", F.year("recall_date"))
        .withColumn("recall_month", F.month("recall_date"))
        .withColumn("recall_quarter", F.quarter("recall_date"))
    )


def add_data_quality_flags(df: DataFrame) -> DataFrame:
    """Add boolean flags for data quality issues."""
    return (
        df
        .withColumn("dq_missing_state", F.col("state").isNull() | (F.trim(F.col("state")) == ""))
        .withColumn("dq_missing_reason", F.col("reason").isNull() | (F.trim(F.col("reason")) == ""))
        .withColumn("dq_missing_classification", F.col("classification").isNull())
        .withColumn("dq_missing_recall_date", F.col("recall_date").isNull())
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records.
    For FDA: deduplicate on recall_id, keep latest by report_date.
    For FSIS: recall_id is generated, so duplicates come from
              identical records across daily ingestions.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("recall_id").orderBy(F.col("report_date").desc())
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# =============================================================================
# MAIN
# =============================================================================

def run(bronze_bucket: str, silver_bucket: str, date_str: str):
    """
    Main Silver transform pipeline.

    1. Read FDA and FSIS Bronze JSON from GCS
    2. Normalize both into unified schema
    3. Union
    4. Clean, enrich, parse contaminants
    5. Write Parquet to GCS Silver
    """
    spark = create_spark_session()

    date = datetime.strptime(date_str, "%Y-%m-%d")
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")

    bronze_path_fda = f"{bronze_bucket}/raw/fda/year={year}/month={month}/day={day}/data.json"
    bronze_path_fsis = f"{bronze_bucket}/raw/fsis/year={year}/month={month}/day={day}/data.json"

    # ── Read Bronze ──────────────────────────────────────────────────────
    print(f"Reading FDA Bronze from: {bronze_path_fda}")
    df_fda_raw = spark.read.option("multiline", "false").json(bronze_path_fda)
    print(f"  FDA raw records: {df_fda_raw.count()}")

    print(f"Reading FSIS Bronze from: {bronze_path_fsis}")
    df_fsis_raw = spark.read.option("multiline", "false").json(bronze_path_fsis)
    print(f"  FSIS raw records: {df_fsis_raw.count()}")

    # ── Normalize ────────────────────────────────────────────────────────
    print("Normalizing schemas...")
    df_fda = normalize_fda(df_fda_raw)
    df_fsis = normalize_fsis(df_fsis_raw)

    # ── Union ────────────────────────────────────────────────────────────
    print("Unioning datasets...")
    df_unified = df_fda.unionByName(df_fsis)
    print(f"  Unified records: {df_unified.count()}")

    # ── Transform ────────────────────────────────────────────────────────
    print("Normalizing states...")
    df_unified = normalize_state(df_unified)

    print("Parsing contamination categories...")
    df_unified = parse_contamination(df_unified)

    print("Adding time dimensions...")
    df_unified = add_time_dimensions(df_unified)

    print("Adding data quality flags...")
    df_unified = add_data_quality_flags(df_unified)

    print("Deduplicating...")
    df_unified = deduplicate(df_unified)

    final_count = df_unified.count()
    print(f"  Final record count: {final_count}")

    # ── Write Silver ─────────────────────────────────────────────────────
    silver_path = f"{silver_bucket}/silver/food_recalls/year={year}/month={month}/day={day}"
    print(f"Writing Silver to: {silver_path}")

    (
        df_unified
        .repartition(1)  # Single file for small dataset
        .write
        .mode("overwrite")
        .parquet(silver_path)
    )

    print(f"\n✅ Silver transform complete!")
    print(f"   Records: {final_count}")
    print(f"   Output:  {silver_path}")

    # ── Quick stats ──────────────────────────────────────────────────────
    print("\n📊 Quick stats:")
    print("\nBy source:")
    df_unified.groupBy("source_agency").count().show()

    print("By classification:")
    df_unified.groupBy("classification").count().orderBy("count", ascending=False).show()

    print("By contamination category:")
    df_unified.groupBy("contamination_category").count().orderBy("count", ascending=False).show(20)

    print("Data quality flags:")
    for flag in ["dq_missing_state", "dq_missing_reason", "dq_missing_classification", "dq_missing_recall_date"]:
        flagged = df_unified.filter(F.col(flag) == True).count()
        print(f"  {flag}: {flagged}")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver transform for food recall data")
    parser.add_argument("--bronze-bucket", required=True, help="GCS Bronze bucket (e.g., gs://food-recall-bronze)")
    parser.add_argument("--silver-bucket", required=True, help="GCS Silver bucket (e.g., gs://food-recall-bronze)")
    parser.add_argument("--date", required=True, help="Ingest date (YYYY-MM-DD)")

    args = parser.parse_args()
    run(args.bronze_bucket, args.silver_bucket, args.date)
