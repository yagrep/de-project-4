CREATE TABLE IF NOT EXISTS dds.addresses (
	id int4 NOT NULL,
	address varchar NOT NULL,
	CONSTRAINT addresses_pkey PRIMARY KEY (id),
	CONSTRAINT addresses_address_unique UNIQUE(address)
);