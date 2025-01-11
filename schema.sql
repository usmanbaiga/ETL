
CREATE TABLE IF NOT EXISTS meta_table (
    table_name character varying(100),
    key_name character varying(100),
    key_value character varying(100),
    CONSTRAINT meta_table_pkey PRIMARY KEY (table_name, key_name)
);


CREATE TABLE IF NOT EXISTS raw_sales (
    id SERIAL PRIMARY KEY,
    mongo_id character varying(50),
    order_id bigint,
    product_id bigint,
    category_id bigint,
    category_code character varying(1000),
    brand character varying(500),
    user_id bigint,
    price numeric(10,2),
    event_time timestamp with time zone,
    product_hash character varying(50)
);


CREATE TABLE IF NOT EXISTS dim_products (
    id character varying(50) PRIMARY KEY,
    category_code character varying(255),
    brand character varying(255)
);


CREATE TABLE IF NOT EXISTS dim_date (
    date_key integer PRIMARY KEY,
    full_date date,
    year integer,
    quarter integer,
    month integer,
    day integer,
    weekday integer,
    hour integer,
    day_of_week integer,
    is_weekend boolean
);


CREATE TABLE IF NOT EXISTS fact_sales (
    id SERIAL PRIMARY KEY,
    date_key integer REFERENCES dim_date (date_key),
    product_hash character varying(50) REFERENCES dim_products (id),
    sales_count integer,
    price numeric(10,2)
);

    