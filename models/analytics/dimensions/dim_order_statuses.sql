with stg_orders as (

    select distinct
        o.`ID` as order_id,  
        o.`CANCEL_REASON`, 
        o.`FINANCIAL_STATUS`, 
        o.`FULFILLMENT_STATUS`, 
        o.`PROCESSING_METHOD`, 
        o.name,
        o.taxes_included,
        o.currency,
        ol.title
    from {{ ref('stg_shopify__orders') }} o
    left join {{ ref('stg_shopify__order_lines') }} ol on o.id = ol.order_id

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['stg_orders.order_id']) }} as order_key,
        *
    from stg_orders
    
)

select * from final
