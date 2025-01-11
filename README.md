# ETL

Pipeline is designed to skip subsequent tasks incase no new data is picked from source DB. Recent data is identified by maintaining a meta_table, which contains the document/Object ID of last processed record (from previous run).

If you want to trigger the pipeline from scratch, please set the setting `start_from_scratch` in etl.py to `True` and run the DAG. It will force removal of previous data before starting the ETL process. 

As for the report UI, This is my first ever Dashboard in PowerBI.
