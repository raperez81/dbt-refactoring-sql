-- CTEs for Sources
with base_orders as (

    select * from raw.jaffle_shop.orders 

),

base_payments as (

    select * from raw.stripe.payment
),

base_customers as (

    select * from raw.jaffle_shop.customers

),

-- Small transformations like rename columns, etc
orders as (

    select
        id as order_id
        , user_id as customer_id
        , order_date as order_placed_at
        , status as order_status
        , _etl_loaded_at
    from base_orders

),

customers as (
    
    select
        id as customer_id
        , first_name as customer_first_name
        , last_name as customer_last_name
    from base_customers

),

payments as (

    select
        id as payment_id
        , orderid as order_id
        , paymentmethod as payment_method
        , status as payment_status
        , amount as payment_amount
        , created as payment_created
        , _batched_at
    from base_payments

),

-- Staging models
orders_with_finalized_payments as (

    select 
        order_id
        , max(payment_created) as payment_finalized_date
        , sum(payment_amount) / 100.0 as total_amount_paid
    from payments
    where payment_status <> 'fail'
    group by 1

),

paid_orders as (

    select 
        orders.order_id
        , orders.customer_id
        , orders.order_placed_at
        , orders.order_status
        , orders_with_finalized_payments.total_amount_paid
        , orders_with_finalized_payments.payment_finalized_date
        , customers.customer_first_name
        , customers.customer_last_name
    from orders
    left join orders_with_finalized_payments ON orders.order_id = orders_with_finalized_payments.order_id
    left join customers on orders.customer_id = customers.customer_id 

),

customer_orders as (

    select 
        customers.customer_id
        , min(orders.order_placed_at) as first_order_date
        , max(orders.order_placed_at) as most_recent_order_date
        , count(orders.order_id) AS number_of_orders
    from customers
    left join orders on customers.customer_id = orders.customer_id
    group by 1

),
-- todo: rename x to explain better the purpose of the CTE
x as (
    select
        paid_orders.order_id
        , sum(t2.total_amount_paid) as customer_lifetime_value
    from paid_orders
    left join paid_orders t2 
    on paid_orders.customer_id = t2.customer_id 
    and paid_orders.order_id >= t2.order_id
    group by 1
    --order by paid_orders.order_id

)

select
    paid_orders.*
    , row_number() over (order by paid_orders.order_id) as transaction_seq
    , row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq
    , case 
        when customer_orders.first_order_date = paid_orders.order_placed_at
        then 'new'
        else 'return' 
    end as nvsr
    , x.customer_lifetime_value
    , customer_orders.first_order_date as fdos
from paid_orders
left join customer_orders on paid_orders.customer_id = customer_orders.customer_id
left join x on x.order_id = paid_orders.order_id
order by order_id