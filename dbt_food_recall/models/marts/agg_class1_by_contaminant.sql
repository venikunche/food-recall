-- Q: Class I recalls by contamination type (most dangerous incidents)

select
    recall_year,
    contamination_category,
    source_agency,
    count(*) as recall_count

from {{ ref('fct_recalls') }}
where classification = 'Class I'
  and recall_year is not null
group by 1, 2, 3
order by 1, recall_count desc
