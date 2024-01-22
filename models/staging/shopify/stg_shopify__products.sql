{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'products') }}

),

renamed as (

  select
    `ID`,
    `TITLE`,
    `PRODUCT_TYPE`,
    timestamp(left(`CREATED_AT`, length(CREATED_AT)-6)) AS CREATED_AT,
    timestamp(left(`UPDATED_AT`, length(UPDATED_AT)-6)) AS UPDATED_AT,
    timestamp(left(`PUBLISHED_AT`, length(PUBLISHED_AT)-6)) AS PUBLISHED_AT,
    `PUBLISHED_SCOPE`,
    `STATUS`

  from source

)

select * from renamed
