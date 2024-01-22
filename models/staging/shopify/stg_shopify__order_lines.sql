{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'order_lines') }}

),

renamed as (

  select
    `ID`,
    `ORDER_ID`,
    `PRODUCT_ID`,
    `VARIANT_ID`,
    `NAME`,
    `TITLE`,
    cast(replace(`PRICE`, ',', '') as numeric) as PRICE,
    cast(`QUANTITY` as numeric) as QUANTITY,
    cast(`FULFILLABLE_QUANTITY` as numeric) as FULFILLABLE_QUANTITY,
    `INDEX`,
    cast(replace(`TOTAL_DISCOUNT`, ',', '') as decimal) as TOTAL_DISCOUNT,
    cast(replace(`PRE_TAX_PRICE`, ',', '') as decimal) as PRE_TAX_PRICE,
    `FULFILLMENT_STATUS`

  from source

)

select * from renamed
