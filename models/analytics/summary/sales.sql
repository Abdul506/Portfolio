with f_sales as (

    select * from {{ ref('fct_sales') }}

),

d_customers as (

    select * from {{ ref('dim_customers') }}

),

d_date as (

    select * from {{ ref('dim_date') }}

),

d_products as (

    select * from {{ ref('dim_products') }}

),

d_order_statuses as (

    select * from {{ ref('dim_order_statuses') }}

),

d_subs as (

    select * from {{ ref('dim_subs') }}

),

final as (

    select
        {{ dbt_utils.star(
            from=ref('fct_sales'), 
            relation_alias='f_sales', 
            except=["product_key", "customer_key", "order_status_key", "order_date_key", "cancel_reason", "fulfillment_status"]
        ) }},
        {{ dbt_utils.star(from=ref('dim_customers'), relation_alias='d_customers', except=["customer_key"]) }},
        {{ dbt_utils.star(from=ref('dim_date'), relation_alias='d_date', except=["date_key"]) }},
        {{ dbt_utils.star(from=ref('dim_products'), relation_alias='d_products', except=["product_key"]) }},
        {{ dbt_utils.star(from=ref('dim_order_statuses'), relation_alias='d_order_statuses', except=["order_status_key"]) }},
        {{ dbt_utils.star(from=ref('dim_subs'), relation_alias='d_subs', except=["subscription_key", "customer_key", "customer_id", "currency"]) }}
    from f_sales
    left join d_customers on f_sales.customer_key = d_customers.customer_key
    left join d_date on f_sales.order_date_key = d_date.order_date_key
    left join d_products on f_sales.product_key = d_products.product_key
    left join d_order_statuses on f_sales.order_status_key = d_order_statuses.order_key
    left join d_subs on d_customers.customer_key = d_subs.customer_key

)

select * from final
