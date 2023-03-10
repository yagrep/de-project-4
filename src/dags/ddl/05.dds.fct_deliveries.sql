CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
	id int4 NOT NULL,
	order_id int NOT NULL,
	courier_id int NOT NULL,
	address_id int NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum NUMERIC(19,2) NOT NULL,
	tip_sum NUMERIC(19,2) NOT NULL,
	CONSTRAINT fct_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT fct_deliveries_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.orders(id),
	CONSTRAINT fct_deliveries_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.couriers(id),
	CONSTRAINT fct_deliveries_address_id_fk FOREIGN KEY (address_id) REFERENCES dds.addresses(id)
);