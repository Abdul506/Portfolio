{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'refunds') }}

),

renamed as (

  select
    `ID`,
    timestamp(left(`CREATED_AT`, length(CREATED_AT)-6)) AS CREATED_AT,
    `PROCESSED_AT`,
    `NOTE`,
    `RESTOCK`,
    `ORDER_ID`

  from source

)

select * from renamed
