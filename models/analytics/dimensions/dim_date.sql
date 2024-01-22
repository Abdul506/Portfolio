{{
    config(
        materialized = "table"
    )
}}
with base as (
      
      select
          {{ dbt_utils.generate_surrogate_key(['dates.date_day']) }} as order_date_key,
          date_day
      from {{ ref('dates') }}
  
)

select * from base
