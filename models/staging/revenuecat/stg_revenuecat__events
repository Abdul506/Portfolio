{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('revenuecat', 'revenuecat_events') }}

),

renamed as (

  select
    `ID`,
    timestamp(left(`TIMESTAMP`, length(TIMESTAMP)-6)) as TIMESTAMP,
    `USER_ID`,
    timestamp(left(`EXPIRES_AT`, length(EXPIRES_AT)-6)) as EXPIRES_AT,
    `PRODUCT_ID`,
    `EVENT_NAME`,
    `TRANSACTION_ID`,
    `ORIGINAL_TRANSACTION_ID`,
    `STORE`,
    `REVENUE`,
    `CURRENCY`,
    `CANCELLATION_REASON`

  from source

)

select * from renamed
