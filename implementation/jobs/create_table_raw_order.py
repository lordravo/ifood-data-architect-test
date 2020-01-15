from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, BooleanType, ArrayType
from pyspark.sql.functions import from_json
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


SOURCE_PATH = 's3n://ifood-data-architect-test-source/order.json.gz'
DESTINATION_DATABASE = 'raw_layer'
DESTINATION_TABLE = 'order'

spark = SparkSession.builder.appName("ifoodETL").enableHiveSupport().config(conf=SparkConf()).getOrCreate()


def ensure_database(database_name):
    spark.sql("create database if not exists {0} location '/{0}/'".format(database_name))


def create_table(df, database_name, table_name):
    temp_table = "temp_{0}".format(table_name)
    df.createOrReplaceTempView(temp_table)
    sql_create_table = """
    create table if not exists {0}.{1}
    using parquet
    as SELECT *
    from {2}
    """.format(database_name, table_name, temp_table)
    spark.sql(sql_create_table)


value_schema = StructType([
    StructField("value", StringType()),
    StructField("currency", StringType())
])

garnish_items_schema = ArrayType(
    StructType([
        StructField("name", StringType()),
        StructField("addition", value_schema),
        StructField("discount", value_schema),
        StructField("quantity", DoubleType()),
        StructField("sequence", IntegerType()),
        StructField("unitPrice", value_schema),
        StructField("categoryId", StringType()),
        StructField("externalId", StringType()),
        StructField("totalValue", value_schema),
        StructField("categoryName", StringType()),
        StructField("integrationId", StringType())
    ])
)

items_schema = ArrayType(
    StructType([
        StructField("name", StringType()),
        StructField("addition", value_schema),
        StructField("discount", value_schema),
        StructField("quantity", DoubleType()),
        StructField("sequence", IntegerType()),
        StructField("unitPrice", value_schema),
        StructField("externalId", StringType()),
        StructField("totalValue", value_schema),
        StructField("customerNote", StringType()),
        StructField("garnishItems", garnish_items_schema),
        StructField("integrationId", StringType()),
        StructField("totalAddition", value_schema),
        StructField("totalDiscount", value_schema)
    ])
)

schema = StructType([
    StructField("cpf", StringType()),
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("delivery_address_city", StringType()),
    StructField("delivery_address_country", StringType()),
    StructField("delivery_address_district", StringType()),
    StructField("delivery_address_external_id", StringType()),
    StructField("delivery_address_latitude", StringType()),
    StructField("delivery_address_longitude", StringType()),
    StructField("delivery_address_state", StringType()),
    StructField("delivery_address_zip_code", StringType()),
    StructField("items", StringType()),  # for some reason it is not properly detecting items_schema *1
    StructField("merchant_id", StringType()),
    StructField("merchant_latitude", StringType()),
    StructField("merchant_longitude", StringType()),
    StructField("merchant_timezone", StringType()),
    StructField("order_created_at", TimestampType()),
    StructField("order_id", StringType()),
    StructField("order_scheduled", BooleanType()),
    StructField("order_scheduled_date", TimestampType()),
    StructField("order_total_amount", DoubleType()),
    StructField("origin_platform", StringType())
])


df = spark.read.json(SOURCE_PATH, schema=schema)
df = df.withColumn('items', from_json('items', items_schema))
df = df.dropDuplicates()

ensure_database(DESTINATION_DATABASE)
create_table(df, DESTINATION_DATABASE, DESTINATION_TABLE)

spark.sparkContext.stop()
