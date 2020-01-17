from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

DESTINATION_DATABASE = 'trusted_layer'
DESTINATION_TABLE = 'order'

spark = SparkSession.builder.appName("ifoodETL").enableHiveSupport().config(conf=SparkConf()).getOrCreate()


def ensure_database(database_name):
    spark.sql("CREATE DATABASE IF NOT EXISTS {0} LOCATION '/{0}/'".format(database_name))


def create_temp_view(temp_view_name, query):
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
    create_temp_view('order_validation', query)


def prepare_order_with_status():
    spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
    query = """
    WITH valid_order_final_status AS (
        SELECT
            order_id,
            COALESCE(CAST(CAST(MAX(IF(value = 'REGISTERED', created_at, NULL)) AS DATE) AS STRING), '') AS registration_date,
            SUBSTR(MAX(CONCAT(CAST(created_at AS STRING), '<>', value)), 22) AS status
        FROM raw_layer.order_status
        WHERE order_id IN (SELECT order_id FROM order_validation WHERE valid_flow)
        GROUP BY order_id
    ),
    order_registration AS (
        SELECT
            *,
            COALESCE(CAST(CAST(order_created_at AS DATE) AS STRING), '') AS registration_date
        FROM raw_layer.order
    ),
    order_with_status AS (
        SELECT
            order.`(cpf|customer_name|registration_date)?+.+`,
            SHA2(cpf, 256) AS anon_cpf,
            SHA2(customer_name, 256) AS anon_order_customer_name,
            valid_order_final_status.status AS status
        FROM order_registration AS order
        LEFT JOIN valid_order_final_status ON
            valid_order_final_status.order_id = order.order_id AND
            order.registration_date = valid_order_final_status.registration_date 
        WHERE valid_order_final_status.order_id IS NOT NULL
    )

    SELECT * FROM order_with_status
    """
    create_temp_view('order_with_status', query)


def prepare_anon_consumer():
    spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
    query = """
    SELECT
        consumer.`(customer_name|customer_phone_number|language|created_at|active)?+.+`,
        consumer.language AS customer_language,
        consumer.created_at AS customer_created_at,
        consumer.active AS customer_active,
        SHA2(customer_phone_number, 256) AS anon_customer_phone_number,
        SHA2(customer_name, 256) AS anon_customer_name
    FROM raw_layer.consumer
    """
    create_temp_view('anon_consumer', query)


def prepare_restaurant():
    spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
    query = """
    SELECT
        restaurant.`(created_at|enabled|price_range|average_ticket|takeout_time|delivery_time|minimum_order_value)?+.+`,
        restaurant.created_at AS restaurant_created_at,
        restaurant.enabled AS restaurant_enabled,
        restaurant.price_range AS restaurant_price_range,
        restaurant.average_ticket AS restaurant_average_ticket,
        restaurant.takeout_time AS restaurant_takeout_time,
        restaurant.delivery_time AS restaurant_delivery_time,
        restaurant.minimum_order_value AS restaurant_minimum_order_value
    FROM raw_layer.restaurant
    """
    create_temp_view('restaurant', query)


def prepare_trusted_order():
    spark.sql("SET spark.sql.parser.quotedRegexColumnNames=true")
    query = """
    WITH order_extended AS (
        SELECT
            order_with_status.*,
            restaurant.`(id)?+.+`,
            anon_consumer.`(customer_id)?+.+`
        FROM order_with_status
        LEFT JOIN restaurant ON
            restaurant.id = order_with_status.merchant_id
        LEFT JOIN anon_consumer ON
            anon_consumer.customer_id = order_with_status.customer_id
    ),
    order_localized AS (
        SELECT
            *,
            CAST(FROM_UTC_TIMESTAMP(CAST(order_created_at AS STRING), merchant_timezone) AS DATE) AS merchant_order_created_at
        FROM order_extended
    )
    SELECT * FROM order_localized
    """
    create_temp_view('trusted_order', query)


def create_table_from_view(database_name, table_name, temp_view_name):
    sql_create_table = """
    CREATE TABLE IF NOT EXISTS {0}.{1}
    USING parquet
    PARTITIONED BY (merchant_order_created_at)
    AS SELECT *
    FROM {2}
    """.format(database_name, table_name, temp_view_name)
    spark.sql(sql_create_table)


ensure_database(DESTINATION_DATABASE)

prepare_order_validation()
prepare_order_with_status()
spark.catalog.dropTempView("order_validation")

prepare_anon_consumer()
prepare_restaurant()

prepare_trusted_order()

create_table_from_view(DESTINATION_DATABASE, DESTINATION_TABLE, 'trusted_order')

spark.catalog.dropTempView("order_with_status")
spark.catalog.dropTempView("consumer")
spark.catalog.dropTempView("restaurant")
spark.catalog.dropTempView("trusted_order")

spark.sparkContext.stop()
