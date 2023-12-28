{{
  config(
    materialized='incremental',
    unique_key='id',
  )
}}

with source as (

  select * from {{ source('stripe', 'stripe_charges') }}

),

renamed as (

  select
    `ID`,
    timestamp(left(`CREATED_AT`, length(`CREATED_AT`)-6)) as CREATED_AT,
    timestamp(left(`PROCESSED_AT`, length(`PROCESSED_AT`)-6)) as PROCESSED_AT,
    cast(`AMOUNT` as numeric) as amount,
    cast(`AMOUNT_REFUNDED` as numeric) amount_refunded,
    `CURRENCY`,
    `STATUS`,
    `SUBSCRIPTION_ID`,
    timestamp(left(`SUBSCRIPTION_CREATED_AT`, length(`SUBSCRIPTION_CREATED_AT`)-6)) as SUBSCRIPTION_CREATED_AT,
    timestamp(left(`SUBSCRIPTION_CANCELLED_AT`, length(`SUBSCRIPTION_CANCELLED_AT`)-6)) as SUBSCRIPTION_CANCELLED_AT,
    timestamp(left(`SUBSCRIPTION_BILLING_CYCLE_ANCHOR`, length(`SUBSCRIPTION_BILLING_CYCLE_ANCHOR`)-6)) as SUBSCRIPTION_BILLING_CYCLE_ANCHOR,
    timestamp(left(`SUBSCRIPTION_CURRENT_PERIOD_START`, length(`SUBSCRIPTION_CURRENT_PERIOD_START`)-6)) as SUBSCRIPTION_CURRENT_PERIOD_START,
    timestamp(left(`SUBSCRIPTION_CURRENT_PERIOD_END`, length(`SUBSCRIPTION_CURRENT_PERIOD_END`)-6)) as SUBSCRIPTION_CURRENT_PERIOD_END,
    `CUSTOMER_ID`,
    `SUBSCRIPTION_TYPE`

  from source

)

select * from renamed
