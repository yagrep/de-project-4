CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	courier_id int NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int NOT NULL,
	orders_total_sum NUMERIC(19,2) NOT NULL,
	rate_avg NUMERIC(19,2) NOT NULL,
	order_processing_fee NUMERIC(19,2) NOT NULL,
	courier_order_sum NUMERIC(19,2) NOT NULL,
	courier_tips_sum NUMERIC(19,2) NOT NULL,
	courier_reward_sum NUMERIC(19,2) NOT NULL,
	CONSTRAINT cdm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT cdm_courier_ledger_unique UNIQUE(courier_id, settlement_year, settlement_month)
);