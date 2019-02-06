with orders as (
	
	select * from 
		{{ref('stg_orders')}}
	where state != 'canceled' 
		and extract(year from orders.completed_at) < '2018' 
		and orders.email not like '%company.com' 
	), 

order_items as (

	select * from 
		{{ref('stg_order_items')}}

		),

with order_totals as (

	select 
		id
		, number
		, completed_at
		, completed_at::date as completed_at_date
		, sum(orders.total) as net_rev
		, sum(orders.item_total) as gross_rev
		, count(orders.id) as order_count
from orders
group by 1
		, 2
		, 3

			), 

orders_complete as (

select 
	order_items.order_id
		, orders.completed_at::date as completed_at_date
		, sum(order_items.quantity) as qty 
	from order_items 
	left join source_data.orders on order_items.order_id = orders.id 
	where (orders.is_cancelled_order = false or orders.is_pending_order != true) 
	group by 
		1
	),

Joined as (

select 
	order_items.completed_at_date
	, order_items.gross_rev
	, order_items.net_rev
	, order_totals.qty
	, order_items.order_count as orders
	, order_totals.qty/order_items.distinct_orders as avg_unit_per_order
	, order_items.Gross_Rev/order_items.distinct_orders as aov_gross
	, order_items.Net_Rev/order_items.distinct_orders as aov_net

from 

oin order_items 
	on 
		orders_complete.completed_at_date = order_items.completed_at_date
where order_items.net_rev >= 150000
order by 
	completed_at_date desc

