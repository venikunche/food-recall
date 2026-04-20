-- Central fact table: one row per recall event.
-- Partitioned by recall_date (month) for time-series queries.
-- Clustered by classification and state for common filters.

{{
  config(
    materialized='table',
    partition_by={
      "field": "recall_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["classification", "state"]
  )
}}

select
    recall_id,
    source_agency,
    classification,
    reason,
    contamination_category,
    recall_date,
    report_date,
    recall_year,
    recall_month,
    recall_quarter,
    company,
    state,
    city,
    product_description,
    status,
    voluntary_mandated,
    distribution_pattern,
    product_quantity,

    -- Contamination boolean flags
    contam_salmonella,
    contam_listeria,
    contam_ecoli,
    contam_undeclared_milk,
    contam_undeclared_peanut,
    contam_undeclared_soy,
    contam_undeclared_egg,
    contam_undeclared_wheat,
    contam_undeclared_tree_nuts,
    contam_undeclared_allergen_other,
    contam_foreign_object,
    contam_mislabeling,

    -- Data quality
    dq_missing_state,
    dq_missing_reason,
    dq_missing_classification,
    dq_missing_recall_date

from {{ ref('stg_food_recalls') }}
