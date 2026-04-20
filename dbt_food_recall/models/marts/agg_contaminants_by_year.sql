-- Q: Which contaminants are most common? Which are growing fastest?

select
    recall_year,
    contamination_category,
    count(*) as recall_count,
    round(
        count(*) * 100.0 / sum(count(*)) over (partition by recall_year),
        2
    ) as pct_of_year

from {{ ref('fct_recalls') }}
where recall_year is not null
group by 1, 2
order by 1, 2
