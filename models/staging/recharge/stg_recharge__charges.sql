{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('recharge', 'refund_charges') }}

),

renamed as (

  select
    `ID`,
    timestamp(left(`CREATED_AT`, length(`CREATED_AT`)-6)) as CREATED_AT,
    timestamp(left(`UPDATED_AT`, length(`UPDATED_AT`)-6)) as UPDATED_AT,
    timestamp(left(`SCHEDULED_AT`, length(`SCHEDULED_AT`)-6)) as SCHEDULED_AT,
    timestamp(left(`PROCESSED_AT`, length(`PROCESSED_AT`)-6)) as PROCESSED_AT,
    cast(parse_numeric(`SUBTOTAL_PRICE`) as numeric) as SUBTOTAL_PRICE,
    cast(`QUANTITY` as numeric) as quantity,
    `STATUS`,
    `PAYMENT_PROCESSOR`,
    `CHARGE_TYPE`,
    `SUBSCRIPTION_ID`,
    `CUSTOMER_ID`,
    timestamp(left(`SUBSCRIPTION_CREATED_AT`, length(`SUBSCRIPTION_CREATED_AT`)-6)) as SUBSCRIPTION_CREATED_AT,
    timestamp(left(`SUBSCRIPTION_CANCELLED_AT`, length(`SUBSCRIPTION_CANCELLED_AT`)-6)) as SUBSCRIPTION_CANCELLED_AT,
    `SUBSCRIPTION_TYPE`

  from source

)

select * from renamed
