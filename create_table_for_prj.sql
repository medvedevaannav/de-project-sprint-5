DROP table if exists stg.orderdelivery_couriers;
DROP table if exists stg.orderdelivery_deliveries;
DROP table if exists dds.dm_deliveries;
DROP table if exists dds.dm_couriers;
DROP table if exists cdm.dm_courier_ledger;
--STG

CREATE TABLE stg.orderdelivery_couriers (
	id serial4 NOT NULL,
	message varchar NULL,
	update_ts timestamp NOT NULL DEFAULT now(),
	CONSTRAINT orderdelivery_couriers_pkey PRIMARY KEY (id)
);


CREATE TABLE stg.orderdelivery_deliveries (
	id serial4 NOT NULL,
	message varchar NULL,
	update_ts timestamp NOT NULL DEFAULT now(),
	CONSTRAINT orderdelivery_deliveries_pkey PRIMARY KEY (id)
);
CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL,
	courier_id varchar NULL,
	courier_name varchar NOT NULL,
	load_dt timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dm_couriers_courier_id UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);
--dds
CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,--оставила тут, т.к. из посновки не ясно,одна достака(delivery_id) может досовлять несколько заказов, или один заказ может доставляться частями-поэтому индекс вклбчает три поля
	order_id varchar NOT NULL,
	order_ts timestamp NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate numeric(14, 2) NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip_sum numeric(14, 2) NOT NULL,
	load_dt timestamp NOT NULL DEFAULT now(),
	CONSTRAINT dm_deliveries_id_uindex UNIQUE (delivery_id, courier_id, order_id),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	constraint dm_deliveries_courier_id_fkey foreign key (courier_id) references dds.dm_couriers (courier_id)
);

--витрина
CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int2 NOT NULL,
	settlement_month int2 NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_courier_ledger_count UNIQUE (courier_id, settlement_year, settlement_month),
	CONSTRAINT dm_courier_ledger_month_check CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id),
	CONSTRAINT dm_courier_ledger_year_check CHECK (((settlement_year >= 1900) AND (settlement_year < 2500)))
);
