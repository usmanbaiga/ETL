from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from bson.objectid import ObjectId
from psycopg2.extras import execute_batch
from time import time_ns
import datetime
import encoding_function as ef
import pandas as pd
import psycopg2
import pymongo


settings = {
    'start_from_scratch': False,
    'batch_size': 1000,
    'meta_table_name': 'meta_table',
    'sales_table': 'raw_sales',
    'sales_key_name': 'last_processed_id',
    'sales_id_column': 'id',
    'raw_sales_max_id_before_current_run': 0,
    'raw_sales_max_id_of_processed_records': 0,
    'sales_table_duplicate_removed': 'non_duplicate_sales',
    'min_date_for_valid_records': '2020-01-01'
}
# Connections
MONGO_CONNECTION_ID = None
POSTGRES_CONNECTION_ID = None

mongo_client = pymongo.MongoClient("mongodb://192.168.18.15:27017/")  # SUCCESS
#mongo_client = pymongo.MongoClient("mongodb://etl_user:aP8fwfgftempRhkgGa9@3.251.75.195:27017/?authSource=sales")  # SUCCESS
db = mongo_client["sales"]
collection = db["sales"]

# PostgreSQL Connection
pg_conn = psycopg2.connect(
    database="tap_test", user="postgres", host='192.168.18.15', password="postgres", port=5432
    #database="sales_db1", user="usman", password="usman", host='rds-module.cnc6gugkeq4f.eu-west-1.rds.amazonaws.com', port=5432
)
pg_cursor = pg_conn.cursor()

# Define the required fields
required_fields = ['order_id', 'product_id', 'category_id', 'category_code', 'brand', 'user_id', 'price', 'event_time']


# Abandoned in favour of mogrified version
def load_data_without_identifying_dupicates(df):
    insert_query = """
    INSERT INTO raw_sales (order_id, product_id, category_id, category_code, brand, user_id, price, event_time)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    data = df[['order_id', 'product_id', 'category_id', 'category_code', 'brand', 'user_id', 'price', 'event_time']].values.tolist()
    execute_batch(pg_cursor, insert_query, data)
    pg_conn.commit()


def truncate_postgres_table_reset_id(table_name):
    pg_cursor.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY;")
    pg_conn.commit()


def drop_tables_in_destination():
    pg_cursor.execute("""
        DROP TABLE IF EXISTS raw_sales;
        DROP TABLE IF EXISTS meta_table;
        DROP TABLE IF EXISTS fact_sales;
        DROP TABLE IF EXISTS dim_products;
        DROP TABLE IF EXISTS dim_date;
        """)
    pg_conn.commit()


def create_tables_in_destination():
    if settings['start_from_scratch']:
        drop_tables_in_destination()
    pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS raw_sales ( id SERIAL PRIMARY KEY, mongo_id character varying(50), order_id bigint, product_id bigint, category_id bigint, category_code character varying(1000), brand character varying(500), user_id bigint, price numeric(10,2), event_time timestamp with time zone, product_hash character varying(50));
    CREATE TABLE IF NOT EXISTS dim_products ( id character varying(50) PRIMARY KEY, category_code character varying(255), brand character varying(255) );
    CREATE TABLE IF NOT EXISTS dim_date ( date_key integer PRIMARY KEY, full_date date, year integer, quarter integer, month integer, day integer, weekday integer, hour integer, day_of_week integer, is_weekend boolean);
    CREATE TABLE IF NOT EXISTS fact_sales ( id SERIAL PRIMARY KEY, date_key integer REFERENCES dim_date (date_key), product_hash character varying(50) REFERENCES dim_products (id), sales_count integer, price numeric(10,2) );
    CREATE TABLE IF NOT EXISTS meta_table ( table_name character varying(100), key_name character varying(100), key_value character varying(100), CONSTRAINT meta_table_pkey PRIMARY KEY (table_name, key_name) );
    """)
    pg_conn.commit()


# Abandoned in favour of encoding
def postgres_get_product_id_from_dimension_table(record):
    p_id = record["product_id"]
    c_id = record["category_id"]
    p_id = postgres_get_one_value(f"select id from dim_products where product_id={p_id} AND category_id={c_id}")
    if p_id is None or p_id == 0:
        # generate new id
        pg_cursor.execute(
            """Insert into dim_products (product_id, category_id, category_code, brand) 
            values (%s, %s, %s, %s)
            RETURNING id""", [record["product_id"], record["category_id"], record["category_code"], record["brand"]])
        p_id = pg_cursor.fetchone()[0]
        pg_conn.commit()
    return p_id


def get_last_element(string):
    parts = string.split(".")
    # Return the last element
    return parts[-1] if parts else None


def transform_document_to_tuple(record):
    # we can generate a unique hash product_id, which will be more efficient than inserting product_id, then returning and using the inserted_id
    # p_id = postgres_get_product_id_from_dimension_table(record["product_id"], record["category_id"])
    p_hash = ef.encode_number(record['product_id'])
    # since we are going with hr granularity, we can compute the date_key from actual date, we will do that during loading process, eliminating the need of roundtrips to DB for insertion/retrival
    return (
            str(record["_id"]),
            record["order_id"],
            record["product_id"],
            record["category_id"],
            get_last_element(record["category_code"]),
            record["brand"],
            float(record["price"]),
            record["user_id"],
            datetime.datetime.strptime(record["event_time"], "%Y-%m-%d %H:%M:%S UTC"),
            p_hash
        )


def get_documents_from_mongo():
    p_id = fetch_meta_table_key(settings['sales_table'], settings['sales_key_name'])
    raw_sales_table_count = postgres_get_table_row_count(settings['sales_table'])
    raw_sales_current_max_id = 0
    if p_id is None or raw_sales_table_count == 0:
        # First time processing
        print("First time processing")
        truncate_postgres_table_reset_id(settings['sales_table'])
        cursor = collection.find().sort("_id", pymongo.ASCENDING).batch_size(settings['batch_size'])
    else:
        # subsequent processing
        print(f"subsequent processing from id: {p_id}")
        raw_sales_current_max_id = postgres_get_table_max_id(settings['sales_table'], settings['sales_id_column'])
        cursor = collection.find({"_id": {"$gt": ObjectId(p_id)}}).sort("_id", pymongo.ASCENDING).batch_size(settings['batch_size'])
    return raw_sales_current_max_id, cursor


def iterate_over_cursor_insert_batch_mogrify(**context):
    last_processed_object_id = None
    batch_to_dump = []
    # do we need to identify the unprocessed records from previous 'incomplete' processing and process them?
    # if YES -> we will have to keep a flag 'is_processed' in raw_sales
    #           get max(id) from raw_sales where is_processed = true
    #           initiate the rest of elt pipeline on these records
    #           which will update the flag against currently marked unprocessed records
    # Proceed with current execution
    raw_sales_current_max_id, mongo_cursor = get_documents_from_mongo()
    # we will use this raw_sales_current_max_id to process ONLY the new records added to raw_sales by this run
    settings['raw_sales_max_id_before_current_run'] = raw_sales_current_max_id
    # saved it be used in subsequent dags/function
    # we can push it to xcomm too

    total = 0
    valid = 0
    # Process documents in batches
    for document in mongo_cursor:
        total += 1
        # process only valid documents with all fields
        if all(field in document for field in required_fields):
            last_processed_object_id = str(document['_id'])
            valid += 1
            batch_to_dump.append(transform_document_to_tuple(document))
            if valid % settings['batch_size'] == 0:
                dump_mogrified_batch_to_postgres(batch_to_dump)
                insert_update_meta_table_key(settings['sales_table'], settings['sales_key_name'],
                                             last_processed_object_id)
                batch_to_dump = []
    # processes remaining items
    if len(batch_to_dump) > 0:
        dump_mogrified_batch_to_postgres(batch_to_dump)
        insert_update_meta_table_key(settings['sales_table'], settings['sales_key_name'],
                                     last_processed_object_id)
    print(f"{valid} of {total} documents dumped to postgres, ID of last processed document {last_processed_object_id}")
    # preserve value for future decision-making
    context['task_instance'].xcom_push(key="valid_message_count", value=valid)


def dump_mogrified_batch_to_postgres(mogrified_batch):
    mogrified_args = ','.join(pg_cursor.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", i).decode('utf-8')
                              for i in mogrified_batch)
    pg_cursor.execute("INSERT INTO "+settings['sales_table']+" (mongo_id, order_id, product_id, category_id, category_code, brand, price, user_id, event_time, product_hash) VALUES " + (mogrified_args))
    pg_conn.commit()


def postgres_get_table_max_id(table_name, col_name):
    return postgres_get_one_value(f"select max({col_name}) from {table_name};")


def postgres_get_one_value(query="select 1"):
    pg_cursor.execute(query)
    result = pg_cursor.fetchone()
    row_count = 0
    if result:
        row_count = result[0]
        if row_count is None:
            row_count = 0
    return row_count


def postgres_get_table_row_count(table_name):
    return postgres_get_one_value(f"select count(1) from {table_name};")


def insert_update_meta_table_key(table_name, key_name, key_value):
    pg_cursor.execute("Insert into meta_table (table_name, key_name, key_value) values (%s, %s, %s) ON CONFLICT (table_name, key_name) DO UPDATE SET key_value = EXCLUDED.key_value", [table_name, key_name, key_value])
    pg_conn.commit()


def processing_remove_duplicates():
    # remove
    #  - duplicates
    #  - too old records
    # Generate
    #  - date_key from event_time
    pg_cursor.execute(f"""
    DROP TABLE IF EXISTS  {settings['sales_table_duplicate_removed']};
    CREATE TABLE {settings['sales_table_duplicate_removed']} AS
    SELECT distinct event_time,order_id,product_id,category_id,category_code,brand,user_id,price,product_hash,TO_CHAR(event_time, 'yyyymmddHH24')::INT AS date_key
    FROM {settings['sales_table']}
    WHERE {settings['sales_id_column']} > {settings['raw_sales_max_id_before_current_run']}
        AND event_time>'{settings['min_date_for_valid_records']}';
    """)
    pg_conn.commit()


def populate_date_dimension():
    pg_cursor.execute(f"SELECT date(min(event_time)), date(max(event_time)) FROM {settings['sales_table_duplicate_removed']}")
    result = pg_cursor.fetchone()
    min_date = '2020-01-01'
    max_date = '2020-12-31'
    if result:
        min_date = result[0]
        max_date = result[1]
    print(min_date, max_date)
    if min_date is None or max_date is None:
        print(f"Nothing to process in temp table {settings['sales_table_duplicate_removed']}")
        return
    pg_cursor.execute(f"""
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
        '{min_date} 00:00:00'::timestamp,
        '{max_date} 23:00:00'::timestamp,
        '1 hour'::interval
    ) AS gs
On CONFLICT (date_key) DO NOTHING;
    """)
    pg_conn.commit()


# DONE: FIX this according to new dim_products table
def populate_product_dimension():
    query = """MERGE INTO dim_products TGT
    USING (
        SELECT 
            product_hash, 
            brand, 
            category_code
        FROM non_duplicate_sales
        GROUP BY product_hash, brand, category_code
    ) AS SRC
    ON SRC.product_hash = TGT.id
    WHEN MATCHED THEN
        UPDATE SET 
            brand = SRC.brand,
            category_code = SRC.category_code
    WHEN NOT MATCHED THEN
        INSERT (id, brand, category_code)
        VALUES (SRC.product_hash, SRC.brand, SRC.category_code);
    """
    pg_cursor.execute(query)
    pg_conn.commit()


def populate_fact_table():
    query = """MERGE INTO fact_sales fc
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
    VALUES (SRC.product_hash, SRC.price, SRC.date_key, SRC.sales_count);"""
    pg_cursor.execute(query)
    pg_conn.commit()


def fetch_meta_table_key(table_name, key_name):
    return postgres_get_one_value(f"select key_value from meta_table where table_name='{table_name}' and key_name='{key_name}';")


"""
start_time = time_ns()
drop_tables_in_destination()
create_tables_in_destination()
print("create_postgres_tables DONE")
iterate_over_cursor_insert_batch_mogrify()
print("iterate_over_cursor_insert_batch_mogrify DONE")
processing_remove_duplicates()
print("processing_remove_duplicates DONE")
populate_date_dimension()
print("populate_date_dimension DONE")
populate_product_dimension()
print("populate_product_dimension DONE")
populate_fact_table()
print("populate_fact_table DONE")
print(f"{(time_ns() - start_time)/1000000000} seconds consumed")
"""


def branch_task(**context):
    valid_message_count = context['task_instance'].xcom_pull(
        task_ids='start_etl',
        key='valid_message_count'
    )
    if valid_message_count is None or valid_message_count == 0:
        return 'stop_processing'
    else:
        return 'continue_processing'


def stop_processing():
    print("Stop further processing as we have no new data.")


# Define the DAG
dag = DAG(
    dag_id='ETL_mongo_to_postgres',
    description='ETL recent data, if none is found stop the execution.',
    schedule_interval='@daily',  # Runs daily
    start_date=datetime.datetime(2025, 1, 10),
    catchup=False,
)

# Start ETL
initialize_etl = PythonOperator(
    task_id='initialize_etl',
    python_callable=create_tables_in_destination,
    dag=dag,
)

start_etl = PythonOperator(
    task_id='start_etl',
    python_callable=iterate_over_cursor_insert_batch_mogrify,
    provide_context=True,  # mandatory for passing context to python_callable function
    dag=dag,
)

check_for_updated_data = BranchPythonOperator(
    task_id='check_for_updated_data',
    python_callable=branch_task,
    provide_context=True,
    dag=dag,
)

populate_date_dimension = PythonOperator(
    task_id='populate_date_dimension',
    python_callable=populate_date_dimension,
    dag=dag,
)

populate_product_dimension = PythonOperator(
    task_id='populate_product_dimension',
    python_callable=populate_product_dimension,
    dag=dag,
)

populate_fact_table = PythonOperator(
    task_id='populate_fact_table',
    python_callable=populate_fact_table,
    dag=dag,
)

stop_processing = PythonOperator(
    task_id='stop_processing',
    python_callable=stop_processing,
    dag=dag,
)

continue_processing = PythonOperator(
    task_id='continue_processing',
    python_callable=processing_remove_duplicates,
    dag=dag,
)

# Define task dependencies
initialize_etl >> start_etl >> check_for_updated_data
check_for_updated_data >> stop_processing
check_for_updated_data >> continue_processing >> populate_date_dimension >> populate_product_dimension >> populate_fact_table
