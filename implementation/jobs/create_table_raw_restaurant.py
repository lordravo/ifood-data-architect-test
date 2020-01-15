from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, TimestampType, BooleanType
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


SOURCE_PATH = 's3n://ifood-data-architect-test-source/restaurant.csv.gz'
DESTINATION_DATABASE = 'raw_layer'
DESTINATION_TABLE = 'restaurant'

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


schema = StructType([
    StructField("id", StringType()),
    StructField("created_at", TimestampType()),
    StructField("enabled", BooleanType()),
    StructField("price_range", StringType()),
    StructField("average_ticket", DoubleType()),
    StructField("takeout_time", IntegerType()),
    StructField("delivery_time", IntegerType()),
    StructField("minimum_order_value", DoubleType()),
    StructField("merchant_zip_code", StringType()),
    StructField("merchant_city", StringType()),
    StructField("merchant_state", StringType()),
    StructField("merchant_country", StringType())
])


df = spark.read.csv(SOURCE_PATH, header=True, schema=schema)
df = df.dropDuplicates()

ensure_database(DESTINATION_DATABASE)
create_table(df, DESTINATION_DATABASE, DESTINATION_TABLE)

spark.sparkContext.stop()
