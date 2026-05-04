{{
  config(
    materialized='table',
    cluster_by=["indicator_code"]
  )
}}

-- Country-level summary: latest rate, trend direction, and global rank
with latest_year as (
    select
        country_code,
        country_name,
        indicator_code,
        indicator_name,
        year,
        rate_pct,
        -- Rank countries worst to best within each indicator
        RANK() OVER (
            PARTITION BY indicator_code
            ORDER BY rate_pct ASC
        ) as global_rank_worst_first
    from {{ ref('fact_bf_rates') }}
    -- Only keep each country's most recent data point
    qualify ROW_NUMBER() OVER (
        PARTITION BY country_code, indicator_code
        ORDER BY year DESC
    ) = 1
),
with_benchmark as (
    select
        *,
        AVG(rate_pct) OVER (PARTITION BY indicator_code) as global_avg_rate,
        -- Flag countries below WHO target of 50% exclusive BF
        CASE
            WHEN indicator_code = 'NT_BF_EXBF' AND rate_pct < 50
            THEN 'Below WHO target'
            WHEN indicator_code = 'NT_BF_EXBF' AND rate_pct >= 50
            THEN 'Meets WHO target'
            ELSE NULL
        END as who_target_status
    from latest_year
)
select * from with_benchmark
