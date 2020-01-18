from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

DESTINATION_DATABASE = 'trusted_layer'
DESTINATION_TABLE = 'order_item'

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


def prepare_order_item():
    query = """
    WITH items_exploded AS (
        SELECT
            order_id,
            EXPLODE(items) AS item
        FROM raw_layer.order 
        WHERE order_id IN (SELECT order_id FROM order_validation WHERE valid_flow)
    ),

    garnish_exploded AS (
        SELECT
            order_id,
            item.externalId AS external_id,
            item.name AS name,
            item.addition.currency AS currency,
            item.addition.value/100 AS addition_value,
            item.discount.value/100 AS discount_value,
            item.quantity AS quantity,
            item.sequence AS sequence,
            item.unitPrice.value/100 AS unit_price_value,
            item.totalValue.value/100 AS total_value,
            item.customerNote AS customer_note,
            EXPLODE(item.garnishItems) AS garnish_item,
            item.integrationId AS integration_id,
            item.totalAddition.value/100 AS total_addition_value,
            item.totalDiscount.value/100 AS total_discount_value
        FROM items_exploded
    ),

    item_garnishes AS (
        SELECT
            order_id,
            external_id,
            name,
            currency,
            addition_value,
            discount_value,
            quantity,
            sequence,
            unit_price_value,
            total_value,
            customer_note,
            integration_id,
            total_addition_value,
            total_discount_value,
            garnish_item.name AS garnish_name,
            garnish_item.externalId AS garnish_external_id,
            garnish_item.categoryId AS garnish_category_id,
            garnish_item.categoryName AS garnish_category_name,
            garnish_item.addition.value/100 AS garnish_addition_value,
            garnish_item.discount.value/100 AS garnish_discount_value,
            garnish_item.quantity AS garnish_quantity,
            garnish_item.sequence AS garnish_sequence,
            garnish_item.unitPrice.value/100 AS garnish_unit_price_value,
            garnish_item.totalValue.value/100 AS garnish_total_value,
            garnish_item.integrationId AS garnish_integration_id
        FROM garnish_exploded
    )

    SELECT * FROM item_garnishes
    """
    create_tmp_view('order_item', query)


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
prepare_order_item()
spark.catalog.dropTempView("order_validation")

create_table_from_view(DESTINATION_DATABASE, DESTINATION_TABLE, 'order_item')

spark.sparkContext.stop()
