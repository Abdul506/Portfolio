with first_time_sub_sales as (

    select 
        sum(case when subscriber_type = 'first subscriber' then pre_tax_price else 0 end) as first_time_sub_sales,
        sum(pre_tax_price) as total_sales,
        round((sum(case when subscriber_type = 'first subscriber' then pre_tax_price else 0 end)/sum(pre_tax_price))*100, 2)
    from {{ ref('sales') }}

)

select * from first_time_sub_sales
