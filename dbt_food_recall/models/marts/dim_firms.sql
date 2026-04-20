-- Dimension: recalling firms
-- One row per unique company + state combination

with firm_stats as (
    select
        company,
        state,
        count(*) as total_recalls,
        min(recall_date) as first_recall_date,
        max(recall_date) as last_recall_date

    from {{ ref('stg_food_recalls') }}
    where company is not null
    group by company, state
)

select
    {{ dbt_utils.generate_surrogate_key(['company', 'coalesce(state, \'UNKNOWN\')']) }} as firm_id,
    company,
    state,
    total_recalls,
    first_recall_date,
    last_recall_date

from firm_stats
