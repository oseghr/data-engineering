{{
  config(
    materialized='table',
    partition_by={
      "field": "year",
      "data_type": "int64",
      "range": {"start": 2000, "end": 2025, "interval": 1}
    },
    cluster_by=["indicator_code", "country_code"]
  )
}}

select
    {{ dbt_utils.generate_surrogate_key(['country_code', 'indicator_code', 'year']) }} as record_id,
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    year,
    rate_pct,
    residence_type,
    wealth_quintile,
    data_source,
    current_timestamp() as loaded_at
from {{ ref('stg_breastfeeding') }}
