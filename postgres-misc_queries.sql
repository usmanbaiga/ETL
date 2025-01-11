-- query for testing purpose ONLY

SELECT order_id, product_id, category_id, category_code, brand, user_id, price,
	event_time, count(1)
	FROM public.raw_sales
	group by 1,2,3,4,5,6,7, 8
	having count(1)>2;

SELECT concat('a', 'b');

select version();

SELECT order_id, product_id, category_id, user_id, count(1)
	FROM public.raw_sales
	group by 1,2,3,4
    having count(1)>2;

SELECT order_id, product_id, category_id, category_code, brand, user_id, price, event_time
	count(1), array_to_string(array_agg(event_time), ',')
	FROM public.raw_sales
	group by 1,2,3,4,5,6,7,8
	having count(1)>1;

SELECT order_id, product_id, user_id, count(1)
	FROM public.raw_sales
	group by 1,2,3
    having count(1)>2;

SELECT order_id, product_id, count(1)
	FROM public.raw_sales
	group by 1,2
    having count(1)>2;

SELECT product_id, category_id, count(1)
	FROM public.raw_sales
	group by 1,2
    having count(1)>2;


SELECT order_id, product_id, count(1)
	FROM public.raw_sales
	group by 1,2
    having count(1)>1 limit 50;

SELECT order_id, count(1)
	FROM public.raw_sales
	group by 1
    having count(1)>4


select * from raw_sales where order_id=2307307184133767963;
select * from raw_sales where product_id=2273948314225345410;
-- OBSERVATION:
--      order_id, category_id, category_code, event_time are same
--      product_id, brand, and price are different


-- dim_products (id varchar, category_code varchar, brand varchar)
-- fact_sales (id int, date_key int, product_hash varchar, sales_count, price)
-- non_duplicate_sales ()
CREATE TABLE {settings['sales_table_duplicate_removed']} AS
SELECT distinct event_time,order_id,product_id,category_id,category_code,brand,user_id,price,product_hash,TO_CHAR(event_time, 'yyyymmddHH24')::INT AS date_key,
FROM {settings['sales_table']}


-- identify if a product is mapped to multiple categories
-- this is to verify that each product is unique with category
select A.product_id, count(1) from (
	select distinct product_id, category_id from raw_Sales
) A
group by A.product_id
having count(1)>1;
-- distinct product_id, category_id >>>> 0 counts
-- distinct product_id, brand >>>> 0 counts

select A.product_id, count(1) from (
	select distinct product_id, brand from raw_Sales
) A
group by A.product_id
having count(1)>1;

truncate table raw_Sales RESTART IDENTITY; 
drop table raw_Sales;
select count(1) from raw_sales;

select min(event_time), max(event_time) from raw_sales;

select count(1), count(distinct event_time) as date_count, min(event_time), max(event_time) from raw_sales where event_time>'1970-01-01 00:33:40';

select date(event_time), count(1) from raw_sales group by 1 order by 1;

select date(event_time), EXTRACT(HOUR FROM event_time), count(1)
from raw_sales
group by 1,2;


select date(event_time), EXTRACT(HOUR FROM event_time), count(1)
from raw_sales
where event_time>='2020-10-10' AND event_time<'2020-10-11'
group by 1,2;


create table non_duplicate_sales AS
select distinct event_time,order_id,product_id,category_id,category_code,brand,user_id,price
from raw_sales;

select
	count(1) as total,
	count(distinct order_id) as o_id,
	count(distinct product_id) as p_id,
	count(distinct category_id) as c_id,
	count(distinct category_code) as c_code,
	count(distinct brand) as brand,
	count(distinct user_id) as users,
	sum(price) as total_sales
from raw_sales where event_time='1970-01-01 00:33:40';

select min(event_time) from raw_sales where event_time > '1970-01-01 00:33:40';

select * from raw_sales order by 1 desc limit 5;

-- this is to verify that each product is of same price
select A.product_id, count(1) from (
	select distinct product_id, price from raw_Sales
) A
group by A.product_id
having count(1)>1;


-- truncate table meta_table;
select * from meta_table;
-- insert into meta_table (table_name, key_name, key_value)
VALUES ('raw_sales', '0');

update meta_table set last_processed_id=1
where table_name='raw_sales';

select * from dim_date limit 5;
select * from dim_products limit 5;



select min(date_key), max(date_key) from dim_date limit 50;
drop table dim_date;

select count(1) from dim_date limit 30;

CREATE TABLE dim_date (
    date_key        SERIAL PRIMARY KEY,        -- Unique identifier for each date
    full_date       DATE,                      -- Full date (yyyy-mm-dd)
    year            INT,                       -- Year
    quarter         INT,                       -- Quarter (1-4)
    month           INT,                       -- Month (1-12)
    day             INT,                       -- Day (1-31)
    weekday         INT,                       -- Weekday (1-7)
    hour            INT,                       -- Hour of the day (00-23)
    day_of_week     INT,                       -- Day of the week (1-7)
    is_weekend      BOOLEAN                    -- Whether the day is a weekend (TRUE/FALSE)
);

-- truncate table dim_products  RESTART IDENTITY CASCADE;
-- truncate table raw_sales RESTART IDENTITY;

-- Insert data into the dim_date table for Jan-2020
INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, weekday, hour, day_of_week, is_weekend)
SELECT
    TO_CHAR(gs, 'yyyymmddHH24')::INT AS date_key,
    gs::DATE AS full_date,
    EXTRACT(YEAR FROM gs) AS year,
    EXTRACT(QUARTER FROM gs) AS quarter,
    EXTRACT(MONTH FROM gs) AS month,
    EXTRACT(DAY FROM gs) AS day,
    EXTRACT(DOW FROM gs) AS weekday,
    TO_CHAR(gs, 'HH24')::INT AS hour,
    EXTRACT(DOW FROM gs) AS day_of_week,
	CASE
        WHEN EXTRACT(DOW FROM gs) IN (0, 6) THEN TRUE  -- 0=Sunday, 6=Saturday
        ELSE FALSE
    END AS is_weekend
FROM generate_series(
        '2020-01-01 00:00:00'::timestamp,
        '2020-01-31 23:00:00'::timestamp,
        '1 hour'::interval
    ) AS gs
On CONFLICT (date_key) DO NOTHING;


select concat(TO_CHAR(gs, 'yyyymmdd'), TO_CHAR(gs, 'HH24')), TO_CHAR(gs, 'yyyymmddHH24'), * from generate_series(
        '2025-01-01 00:00:00'::timestamp,
        '2025-01-31 23:00:00'::timestamp,
        '1 hour'::interval
    ) AS gs;

SELECT date(min(event_time)), date(max(event_time)), count(1) FROM non_duplicate_sales

INSERT INTO director (id, name)
VALUES
    (2, 'robert'),
    (5, 'sheila'),
    (6, 'flora')
ON CONFLICT (id) DO UPDATE
SET name = EXCLUDED.name;


Insert into meta_table (table_name, key_name, key_value) values (%s, %s, %s)
ON CONFLICT (table_name, key_name) DO UPDATE
SET key_value = EXCLUDED.key_value;


-- DROP TABLE IF EXISTS dim_products;

CREATE TABLE IF NOT EXISTS dim_products
(
    id SERIAL PRIMARY KEY,
    product_id bigint NOT NULL,
    category_id bigint,
    category_code character varying(255) COLLATE pg_catalog."default",
    brand character varying(255) COLLATE pg_catalog."default",
	CONSTRAINT dim_products_pid_cid UNIQUE  (product_id, category_id)
)

select * from dim_products limit  5;
select * from non_duplicate_sales limit  5;

Insert into dim_products (product_id, category_id, category_code, brand)
values (%s, %s, %s, %s)
ON CONFLICT (product_id, category_id)
DO UPDATE SET category_code = EXCLUDED.category_code, brand = EXCLUDED.brand


select count(1) from fact_sales limit 5;
select * from non_duplicate_sales limit 5;
-- truncate table non_duplicate_sales ;
select length(product_id::varchar(50)) from non_duplicate_sales limit 5;



-- V1 : not in use anymore
-- CREATE TABLE IF NOT EXISTS dim_products ( id SERIAL PRIMARY KEY, product_id bigint, category_id bigint, category_code character varying(255), brand character varying(255), CONSTRAINT dim_products_pid_cid UNIQUE (product_id, category_id) );

-- V2
CREATE TABLE IF NOT EXISTS dim_products (
        id character varying(50) PRIMARY KEY,
        category_code character varying(255),
        brand character varying(255)
    );


select count(1) from dim_date limit 5
where product_id=2273948314225345410;


-- non_duplicate_sales ()
DROP TABLE IF EXISTS non_duplicate_sales;
CREATE TABLE non_duplicate_sales AS
SELECT distinct event_time,order_id,product_id,category_id,category_code,brand,user_id,price,product_hash,TO_CHAR(event_time, 'yyyymmddHH24')::INT AS date_key
FROM raw_sales
where event_time>'1970-01-02'

select count(1) from fact_sales limit 5;

SELECT product_hash, date_key, count(1) as sale_count, SUM(price) as price
        FROM non_duplicate_sales group by product_hash,date_key


-- Define the foreign key inside the CREATE TABLE statement
CREATE TABLE orders (
    order_id SERIAL,
    dish_name TEXT,
    customer_id INTEGER REFERENCES customers (id)
);

-- Use a separate ALTER TABLE statement
ALTER TABLE orders
    ADD CONSTRAINT fk_orders_customers FOREIGN KEY (customer_id) REFERENCES customers (id);



MERGE INTO fact_sales fc
USING (
    SELECT
        product_hash,
        date_key,
        count(1) as sales_count,
        SUM(price) as price
    FROM non_duplicate_sales
    GROUP BY product_hash,date_key
) AS SRC
ON SRC.date_key = fc.date_key 
   AND SRC.product_hash = fc.product_hash
WHEN MATCHED THEN
    UPDATE SET price = SRC.price, sales_count = SRC.sales_count
WHEN NOT MATCHED THEN
    INSERT (product_hash, price, date_key, sales_count)
    VALUES (SRC.product_hash, SRC.price, SRC.date_key, SRC.sales_count);


MERGE INTO dim_products ph
USING (
	SELECT 
		product_hash, 
		brand, 
		category_code
	FROM non_duplicate_sales
	GROUP BY product_hash, brand, category_code
) AS SRC
ON SRC.product_hash = ph.id
WHEN MATCHED THEN
	UPDATE SET 
		brand = SRC.brand,
		category_code = SRC.category_code
WHEN NOT MATCHED THEN
	INSERT (id, brand, category_code)
	VALUES (SRC.product_hash, SRC.brand, SRC.category_code);


drop table if exists meta_table;
CREATE TABLE IF NOT EXISTS meta_table (
	table_name character varying(100),
	key_name character varying(100),
	key_value character varying(100),
	CONSTRAINT meta_table_pkey PRIMARY KEY (table_name, key_name)
);







-- populate date dimmension from raw_sales
select min(event_time), max(event_time) from raw_sales;
insert into dim_date (date, year, month, day, hour, week, quarter)
select distinct 
	date(event_time),
	EXTRACT(YEAR FROM event_time) as year,
	EXTRACT(MONTH FROM event_time) as month,
	EXTRACT(DAY FROM event_time) as day,
	EXTRACT(HOUR FROM event_time) as hr,
	EXTRACT(WEEK FROM event_time) as week,
	CASE 
        WHEN EXTRACT(MONTH FROM event_time) BETWEEN 1 AND 3 THEN 1
        WHEN EXTRACT(MONTH FROM event_time) BETWEEN 4 AND 6 THEN 2
        WHEN EXTRACT(MONTH FROM event_time) BETWEEN 7 AND 9 THEN 3
        WHEN EXTRACT(MONTH FROM event_time) BETWEEN 10 AND 12 THEN 4
    END AS quarter
from raw_sales order by 2,3,4,5,6;



-- populate products dimmension from raw_sales
insert into dim_products (product_id,category_id,category_code,brand)
select distinct product_id,category_id,category_code,brand from raw_sales order by 1,2,3,4;






