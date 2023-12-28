with orders as (

    select 
        `id` as order_id, 
        `customer_id`, 
        `financial_status`, 
        `fulfillment_status`, 
        `created_at`, 
        `updated_at`, 
        `cancelled_at`, 
        `cancel_reason`, 
        `processed_at`, 
        `name`, 
        `processing_method`, 
        `taxes_included`, 
        `currency` 
    from {{ ref('stg_shopify__orders') }}

),

order_lines as (

    select 
        `id` as order_line_id, 
        `order_id`, 
        `product_id`, 
        `name`, 
        `title`, 
        `price`, 
        `quantity`, 
        `fulfillable_quantity`, 
        `index`, 
        `total_discount`, 
        `pre_tax_price`, 
        `fulfillment_status` 
    from {{ ref('stg_shopify__order_lines') }}

),

final as (

    select 
        {{ dbt_utils.generate_surrogate_key(['order_line_id', 'o.order_id']) }} as sales_key,
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
        {{ dbt_utils.generate_surrogate_key(['created_at']) }} as order_date_key,
        {{ dbt_utils.generate_surrogate_key(['o.fulfillment_status']) }} as order_status_key,
        o.created_at, 
        o.updated_at, 
        o.cancelled_at, 
        o.cancel_reason, 
        o.processed_at, 
        o.fulfillment_status,
        ol.price, 
        ol.quantity, 
        ol.fulfillable_quantity, 
        ol.index, 
        ol.total_discount, 
        ol.pre_tax_price
    from orders as o
    inner join order_lines as ol on o.order_id = ol.order_id


)

select * from final
