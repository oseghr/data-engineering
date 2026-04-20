with source as (
    select * from {{ source('breastfeeding_analytics', 'raw_bf_rates') }}
),
cleaned as (
    select
        REF_AREA                                            as country_code,
        GEOGRAPHIC_AREA                                     as country_name,
        INDICATOR                                           as indicator_code,
        CASE INDICATOR
            WHEN 'NT_BF_EXBF' THEN 'Exclusive breastfeeding (0-5 months)'
            WHEN 'NT_BF_EIBF' THEN 'Early initiation of breastfeeding'
            ELSE INDICATOR
        END                                                 as indicator_name,
        CAST(SPLIT(CAST(TIME_PERIOD as STRING), '-')[OFFSET(0)] as INT64) as year,
        CAST(OBS_VALUE as FLOAT64)                          as rate_pct,
        RESIDENCE                                           as residence_type,
        WEALTH_QUINTILE                                     as wealth_quintile,
        DATA_SOURCE                                         as data_source
    from source
    where OBS_VALUE is not null
      and TIME_PERIOD is not null
      and INDICATOR in ('NT_BF_EXBF', 'NT_BF_EIBF')
)
select * from cleaned
