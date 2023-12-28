with revenuecat as (

    select distinct
        `ORIGINAL_TRANSACTION_ID` as event_id, 
        transaction_id as subscription_id,
        --`ID` as event_id, 
        `USER_ID` as customer_id, 
        max(case when event_name in ('rc_renewal_event', 'rc_initial_purchase_event') then timestamp end) over (partition by transaction_id) as subscription_created_at,
        max(case when event_name in ('rc_cancellation_event') then timestamp end) over (partition by transaction_id) as SUBSCRIPTION_CANCELLED_AT,
        cast(null as timestamp) as SUBSCRIPTION_BILLING_CYCLE_ANCHOR,
        cast(null as timestamp) as SUBSCRIPTION_CURRENT_PERIOD_START,
        max(`EXPIRES_AT`) over (partition by transaction_id) as SUBSCRIPTION_CURRENT_PERIOD_END, 
        `PRODUCT_ID`, 
        `EVENT_NAME`, 
        `STORE`, 
        `REVENUE`, 
        `CURRENCY`, 
        `CANCELLATION_REASON` 
    from {{ ref('stg_revenuecat__events') }}

)

select * from revenuecat
