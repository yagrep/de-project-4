CREATE TABLE IF NOT EXISTS stg.couriers (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_key varchar NOT NULL,
	object_value varchar NOT NULL,
	CONSTRAINT couriers_pkey PRIMARY KEY (id),
	CONSTRAINT couriers_object_key_unique UNIQUE(object_key)
);