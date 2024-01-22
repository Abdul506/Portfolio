{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('shopify', 'orders') }}

),

renamed as (

  select
    `ID`, 
    timestamp(left(`CREATED_AT`, length(CREATED_AT)-6)) AS CREATED_AT,
    timestamp(left(`UPDATED_AT`, length(UPDATED_AT)-6)) AS UPDATED_AT,
    timestamp(left(`CANCELLED_AT`, length(CANCELLED_AT)-6)) as CANCELLED_AT,
    `CANCEL_REASON`, 
    `NAME`, 
    `SHIPPING_ADDRESS_ID`, 
    `BILLING_ADDRESS_ID`, 
    `CUSTOMER_ID`, 
    `FINANCIAL_STATUS`, 
    `FULFILLMENT_STATUS`, 
    timestamp(left(`PROCESSED_AT`, length(PROCESSED_AT)-6)) as PROCESSED_AT,
    `PROCESSING_METHOD`, 
    `TAXES_INCLUDED`, 
    `CURRENCY`
  from source

)

select * from renamed
