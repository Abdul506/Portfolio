{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'customers') }}

),

renamed as (

  select
    `ID`,
    timestamp(left(`CREATED_AT`, length(CREATED_AT)-6)) AS CREATED_AT,
    timestamp(left(`UPDATED_AT`, length(UPDATED_AT)-6)) AS UPDATED_AT,
    `ORDERS_COUNT`,
    `STATE`,
    `TOTAL_SPENT`,
    `CURRENCY`,
    `VERIFIED_EMAIL`

  from source

)

select * from renamed
