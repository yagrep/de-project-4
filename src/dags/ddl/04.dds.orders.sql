CREATE TABLE IF NOT EXISTS dds.orders (
	id int4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	sum NUMERIC(19,2) NOT NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (id),
	CONSTRAINT orders_order_id_unique UNIQUE(order_id)
);