-- Thin staging layer on top of the Silver table.
-- Casts types explicitly, renames for clarity, filters out junk.

with source as (
    select * from {{ source('silver', 'food_recalls') }}
)

select
    recall_id,
    source_agency,
    classification,
    reason,
    recall_date,
    report_date,
    company,
    state,
    city,
    product_description,
    status,
    voluntary_mandated,
    distribution_pattern,
    product_quantity,
    contamination_category,
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
    recall_year,
    recall_month,
    recall_quarter,
    dq_missing_state,
    dq_missing_reason,
    dq_missing_classification,
    dq_missing_recall_date

from source
where recall_id is not null
