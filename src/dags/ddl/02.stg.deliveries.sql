CREATE TABLE IF NOT EXISTS stg.deliveries (
	id int4 NOT NULL GENERATED ALWAYS AS IDENTITY,
	object_key varchar NOT NULL,
	object_value varchar NOT NULL,
	CONSTRAINT deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT deliveries_object_key_unique UNIQUE(object_key)
);