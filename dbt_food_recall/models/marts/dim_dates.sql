-- Date dimension: one row per calendar date in the recall range

with date_spine as (
    select distinct recall_date as date_day
    from {{ ref('stg_food_recalls') }}
    where recall_date is not null
)

select
    date_day,
    extract(year from date_day) as year,
    extract(month from date_day) as month,
    extract(quarter from date_day) as quarter,
    extract(dayofweek from date_day) as day_of_week,
    format_date('%B', date_day) as month_name,
    format_date('%A', date_day) as day_name

from date_spine
