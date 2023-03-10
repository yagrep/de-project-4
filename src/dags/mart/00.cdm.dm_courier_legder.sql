delete from cdm.dm_courier_ledger
where 1 = 1
	and settlement_year = EXTRACT('year' FROM '{{ ds }}'::timestamp)
	and settlement_month = EXTRACT('month' FROM '{{ ds }}'::timestamp)
;

insert into cdm.dm_courier_ledger (
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum
)
with base as (
select
	c.id,
	c.name,
	extract (year from f.order_ts) "settlement_year",
	extract (month from f.order_ts) "settlement_month",
	f.rate,
	f.sum,
	f.tip_sum
from
	dds.fct_deliveries f
left join
	dds.couriers c
	on c.id = f.courier_id
where 1 = 1
	and extract (year from f.order_ts)  = EXTRACT('year' FROM '{{ ds }}'::timestamp)
	and extract (month from f.order_ts) = EXTRACT('month' FROM '{{ ds }}'::timestamp)
), adv as (
select
	distinct
		id,
		name,
		settlement_year,
		settlement_month,
		count(1) over (partition by id, settlement_year, settlement_month rows between  unbounded preceding and unbounded following) "orders_count",
		sum(sum) over (partition by id, settlement_year, settlement_month rows between  unbounded preceding and unbounded following) "orders_total_sum",
		avg(rate) over (partition by id, settlement_year, settlement_month rows between  unbounded preceding and unbounded following) "rate_avg",
		sum(tip_sum) over (partition by id, settlement_year, settlement_month rows between  unbounded preceding and unbounded following) "courier_tips_sum"
from base
), fin as (
select
	id "courier_id",
	name "courier_name",
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	(0.25 * orders_total_sum) "order_processing_fee",
	case
		when rate_avg < 4 then greatest(0.05 * orders_total_sum, 100 * orders_count::numeric)
		when rate_avg >= 4 and rate_avg < 4.5 then greatest(0.07 * orders_total_sum, 150 * orders_count)
		when rate_avg >= 4.5 and rate_avg < 4.9 then greatest(0.08 * orders_total_sum, 175 * orders_count)
		when rate_avg >= 4.9 then greatest(0.1 * orders_total_sum, 200 * orders_count)
	end "courier_order_sum",
	courier_tips_sum
from
	adv
)
select
	*,
	(courier_order_sum + courier_tips_sum * 0.95) "courier_reward_sum"
from fin
;
