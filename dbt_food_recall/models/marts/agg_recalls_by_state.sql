-- Q: Which states have the most recalling firms?

select
    state,
    source_agency,
    count(*) as recall_count,
    count(distinct company) as unique_firms,
    sum(case when classification = 'Class I' then 1 else 0 end) as class_1_count

from {{ ref('fct_recalls') }}
where state is not null
  and state != ''
group by 1, 2
order by recall_count desc
