from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

DESTINATION_DATABASE = 'trusted_layer'
DESTINATION_TABLE = 'order_status'

spark = SparkSession.builder.appName("ifoodETL").enableHiveSupport().config(conf=SparkConf()).getOrCreate()


def ensure_database(database_name):
    spark.sql("CREATE DATABASE IF NOT EXISTS {0} LOCATION '/{0}/'".format(database_name))


def create_tmp_view(temp_view_name, query):
    sql_create_temp_view = """
    CREATE OR REPLACE TEMPORARY VIEW {0} AS
    {1}
    """.format(temp_view_name, query)
    spark.sql(sql_create_temp_view)


def prepare_order_validation():
    query = """
    WITH order_timestamps AS (
        SELECT
            order_id,
            MIN(IF(value = 'REGISTERED', created_at, NULL)) AS registration_time,
            MIN(IF(value = 'PLACED', created_at, NULL)) AS place_time,
            MIN(IF(value = 'CONCLUDED', created_at, NULL)) AS conclusion_time,
            MIN(IF(value = 'CANCELLED', created_at, NULL)) AS cancelation_time
        FROM raw_layer.order_status
        GROUP BY order_id
    ),

    valid_order_flows AS (
        SELECT
            order_id,
            registration_time IS NOT NULL
                AND (
                    (
                    place_time IS NOT NULL
                        AND registration_time <= place_time 
                        AND (
                            (cancelation_time IS NULL AND place_time <= conclusion_time) OR 
                            (conclusion_time IS NULL AND place_time <= cancelation_time)
                        )
                     )
                     OR (
                    cancelation_time IS NOT NULL
                        AND registration_time <= cancelation_time 
                     )
                ) AS valid_flow
        FROM order_timestamps
    ),

    valid_orders AS (
        SELECT
            order_id,
            valid_flow
        FROM valid_order_flows
    )

    SELECT * FROM valid_orders
    """
    create_tmp_view('order_validation', query)


def prepare_order_timestamps():
    query = """
    WITH order_timestamps AS (
        SELECT
            order_id,
            MIN(IF(value = 'REGISTERED', created_at, NULL)) AS registration_time,
            MIN(IF(value = 'PLACED', created_at, NULL)) AS place_time,
            MIN(IF(value = 'CONCLUDED', created_at, NULL)) AS conclusion_time,
            MIN(IF(value = 'CANCELLED', created_at, NULL)) AS cancelation_time
        FROM raw_layer.order_status
        WHERE order_id IN (SELECT order_id FROM order_validation WHERE valid_flow)
        GROUP BY order_id
    )

    SELECT * FROM order_timestamps
    """
    create_tmp_view('order_timestamps', query)


def create_table_from_view(database_name, table_name, temp_view_name):
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS {0}.{1}
    USING parquet
    AS SELECT *
    FROM {2}
    """.format(database_name, table_name, temp_view_name)
    spark.sql(sql_create_table)


ensure_database(DESTINATION_DATABASE)

prepare_order_validation()
prepare_order_timestamps()
spark.catalog.dropTempView("order_validation")

create_table_from_view(DESTINATION_DATABASE, DESTINATION_TABLE, 'order_timestamps')

spark.sparkContext.stop()
