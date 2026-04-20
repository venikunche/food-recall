-- Dimension: contamination types

select distinct
    contamination_category,
    case
        when contamination_category in ('Salmonella', 'Listeria', 'E. coli')
            then 'Biological'
        when contamination_category like 'Undeclared Allergen%'
            then 'Allergen'
        when contamination_category = 'Foreign Object'
            then 'Physical'
        when contamination_category = 'Mislabeling'
            then 'Labeling'
        else 'Other'
    end as contaminant_group

from {{ ref('stg_food_recalls') }}
