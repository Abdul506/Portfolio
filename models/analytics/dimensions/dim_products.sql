with stg_shopify__products as (

    select distinct
        id as product_id,
        product_type,
        title as product_title
    from {{ ref('stg_shopify__products') }}

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['stg_shopify__products.product_id']) }} as product_key,
        product_id, 
        product_type, 
        product_title
    from stg_shopify__products

)

select distinct * from final
