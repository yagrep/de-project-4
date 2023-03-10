CREATE TABLE IF NOT EXISTS dds.deliveries (
	id int4 NOT NULL,
	delivery_id varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	tip_sum NUMERIC(19,2) NOT NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliveries_delivery_id_unique UNIQUE(delivery_id)
);