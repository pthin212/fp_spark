from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType


def create_json_schema():
    return StructType([
        StructField("id", StringType(), True),
        StructField("api_version", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("current_url", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("ip", StringType(), True),
        StructField("local_time", StringType(), True),
        StructField("option", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("referrer_url", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("time_stamp", LongType(), True),
        StructField("user_agent", StringType(), True)
    ])