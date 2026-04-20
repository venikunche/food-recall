-- Q: How have food recalls trended over time (2004–present)?
-- Q: Which recall classifications are growing?

select
    recall_year,
    classification,
    source_agency,
    count(*) as recall_count

from {{ ref('fct_recalls') }}
where recall_year is not null
group by 1, 2, 3
order by 1, 2
