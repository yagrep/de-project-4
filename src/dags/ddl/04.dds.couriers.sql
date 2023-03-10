CREATE TABLE IF NOT EXISTS dds.couriers (
	id int4 NOT NULL,
	courier_id varchar NOT NULL,
	name varchar NOT NULL,
	CONSTRAINT couriers_pkey PRIMARY KEY (id),
	CONSTRAINT couriers_courier_id_unique UNIQUE(courier_id)
);