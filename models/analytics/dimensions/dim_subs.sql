with stripe as (

    select 
        `SUBSCRIPTION_ID`, 
        `ID` as event_id, 
        `CUSTOMER_ID`, 
        `SUBSCRIPTION_CREATED_AT`, 
        `SUBSCRIPTION_CANCELLED_AT`, 
        `SUBSCRIPTION_BILLING_CYCLE_ANCHOR`, 
        `SUBSCRIPTION_CURRENT_PERIOD_START`, 
        `SUBSCRIPTION_CURRENT_PERIOD_END`, 
        `SUBSCRIPTION_TYPE`,
        status,
        currency
    from {{ ref('stg_stripe__charges') }}

),

recharge as (

    select 
        `SUBSCRIPTION_ID`, 
        `ID` as event_id, 
        `CUSTOMER_ID`, 
        `SUBSCRIPTION_CREATED_AT`, 
        `SUBSCRIPTION_CANCELLED_AT`, 
        cast(null as timestamp) as `SUBSCRIPTION_BILLING_CYCLE_ANCHOR`, 
        cast(null as timestamp) as `SUBSCRIPTION_CURRENT_PERIOD_START`, 
        cast(null as timestamp) as `SUBSCRIPTION_CURRENT_PERIOD_END`,
        `SUBSCRIPTION_TYPE`, 
        `STATUS`, 
        cast(null as string) as currency
    from {{ ref('stg_recharge__charges') }}


),

int_subscription_charges as (

    select 
        subscription_id, 
        event_id, 
        customer_id, 
        subscription_created_at, 
        `SUBSCRIPTION_CANCELLED_AT`, 
        `SUBSCRIPTION_BILLING_CYCLE_ANCHOR`, 
        `SUBSCRIPTION_CURRENT_PERIOD_START`, 
        `SUBSCRIPTION_CURRENT_PERIOD_END`, 
        `PRODUCT_ID` as subscription_type, 
        cast(null as string) as status,
        `CURRENCY`, 
    from {{ ref('int_subscription_charges') }}

),

base as (

    select * from stripe

    union all

    select * from recharge

    union all 

    select * from int_subscription_charges


),

agg as (

    select 
        SUBSCRIPTION_ID, 
        {{ dbt_utils.generate_surrogate_key(['subscription_id']) }} as subscription_key,
        event_id, 
        CUSTOMER_ID,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key, 
        SUBSCRIPTION_CREATED_AT, 
        SUBSCRIPTION_CANCELLED_AT, 
        SUBSCRIPTION_BILLING_CYCLE_ANCHOR, 
        SUBSCRIPTION_CURRENT_PERIOD_START, 
        SUBSCRIPTION_CURRENT_PERIOD_END, 
        SUBSCRIPTION_TYPE, 
        count(distinct subscription_id) over (partition by customer_id) as subscription_count,
        status, 
        currency 
    from base

),

final as (

    select 
        `SUBSCRIPTION_ID`, 
        subscription_key, 
        event_id, 
        `CUSTOMER_ID`, 
        customer_key,
        `SUBSCRIPTION_CREATED_AT`, 
        `SUBSCRIPTION_CANCELLED_AT`, 
        `SUBSCRIPTION_BILLING_CYCLE_ANCHOR`, 
        `SUBSCRIPTION_CURRENT_PERIOD_START`, 
        `SUBSCRIPTION_CURRENT_PERIOD_END`, 
        `SUBSCRIPTION_TYPE`, 
        case when subscription_count > 1 then 'returning subscriber' else 'first subscriber' end as subscriber_type, 
        status, 
        currency 
    from agg

)

select * from final
