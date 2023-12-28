with stg_shopify__customers as (

    select 
        id as customer_id,
        state,
        verified_email
    from {{ ref('stg_shopify__customers') }}

),

base as (

    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        c.customer_id,
        c.state,
        c.verified_email
    from stg_shopify__customers as c

)

select * from base
