{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'order_line_refunds') }}

),

renamed as (

  select
    `ID`,
    `REFUND_ID`,
    `RESTOCK_TYPE`,
    `QUANTITY`,
    `SUBTOTAL`,
    `TOTAL_TAX`,
    `ORDER_LINE_ID`

  from source

)

select * from renamed
