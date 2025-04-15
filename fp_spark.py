import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, date_format, to_date
from pyspark.sql.types import StringType, StructType, StructField, LongType, BooleanType
from user_agents import parse
import hashlib
import tldextract
import pycountry
from util.config import Config
from util.logger import Log4j


def hash_string(input_string):
    if input_string is None:
        return None
    encoded_string = input_string.encode('utf-8')
    return hashlib.sha256(encoded_string).hexdigest()


def parse_user_agent(user_agent_string):
    try:
        user_agent = parse(user_agent_string)
        browser_family = user_agent.browser.family
        browser_version = user_agent.browser.version_string
        os_family = user_agent.os.family
        os_version = user_agent.os.version_string

        browser_key = hash_string(browser_family + browser_version)
        os_key = hash_string(os_family + os_version)

        return {
            "browser_family": user_agent.browser.family,
            "browser_version": user_agent.browser.version_string,
            "os_family": user_agent.os.family,
            "os_version": user_agent.os.version_string,
            "browser_key": browser_key,
            "os_key": os_key
        }
    except Exception as e:
        return {
            "browser_family": None,
            "browser_version": None,
            "os_family": None,
            "os_version": None,
            "browser_key": None,
            "os_key": None
        }


def get_tld(url):
    extracted = tldextract.extract(url)
    tld_suffix = extracted.suffix

    tld_parts = tld_suffix.lower().split(".")
    tld_end = tld_parts[-1]

    if tld_end == "uk":
        return tld_end, "United Kingdom"
    elif tld_end == "tr":
        return tld_end, "Turkey"
    else:
        try:
            country = pycountry.countries.get(alpha_2=tld_end.upper())
            return tld_end, country.name
        except Exception as e:
            return tld_end, None


def upsert_dim_table(batch_df, batch_id, table_name, key_columns):
    # Remove duplicates within the current batch
    unique_df = batch_df.dropDuplicates(key_columns)

    # Read existing records from the target PostgreSQL dim table
    existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", jdbc_properties["user"]) \
        .option("password", jdbc_properties["password"]) \
        .option("driver", jdbc_properties["driver"]) \
        .load()

    # Identify new records that dont exist in the dimension table yet
    new_records_df = unique_df.join(existing_df, on=key_columns, how='left_anti')

    # Write only new records to the dimension table
    if new_records_df.count() > 0:
        new_records_df.write \
            .jdbc(url=jdbc_url,
                  table=table_name,
                  mode="append",
                  properties=jdbc_properties)


def write_fact_table(batch_df, batch_id):
    batch_df.write \
        .jdbc(url=jdbc_url,
              table="fact_event",
              mode="append",
              properties=jdbc_properties)


if __name__ == '__main__':
    conf = Config()
    spark_conf = conf.spark_conf
    kaka_conf = conf.kafka_conf
    jdbc_url = conf.postgres_conf["url"]
    jdbc_properties = {
        "user": conf.postgres_conf["user"],
        "password": conf.postgres_conf["password"],
        "driver": conf.postgres_conf["driver"]
    }

    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    log = Log4j(spark)

    log.info(f"spark_conf: {spark_conf.getAll()}")
    log.info(f"kafka_conf: {kaka_conf.items()}")

    df = spark.readStream \
        .format("kafka") \
        .options(**kaka_conf) \
        .load()

    df.printSchema()

    # Define the schema for selected JSON fields
    json_schema = StructType([
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

    # Convert the value column from JSON string to StructType
    df_parsed = df.select(from_json(col("value").cast("string"), json_schema).alias("jsonData"))

    # Register the UDFs
    parse_user_agent_udf = udf(parse_user_agent, StructType([
        StructField("browser_family", StringType(), True),
        StructField("browser_version", StringType(), True),
        StructField("os_family", StringType(), True),
        StructField("os_version", StringType(), True),
        # StructField("device_family", StringType(), True),
        # StructField("is_mobile", BooleanType(), True),
        # StructField("is_pc", BooleanType(), True),
        # StructField("is_tablet", BooleanType(), True),
        StructField("browser_key", StringType(), True),
        StructField("os_key", StringType(), True)
    ]))
    get_tld_udf = udf(get_tld, StructType([
        StructField("tld", StringType(), True),
        StructField("country_name", StringType(), True)
    ]))

    # Enrich the parsed DataFrame with user-agent info and top-level domain info
    df_enriched = df_parsed.withColumn("userAgentInfo", parse_user_agent_udf(col("jsonData.user_agent"))) \
        .withColumn("tldInfo", get_tld_udf(col("jsonData.current_url")))

    # Extract dimension tables
    dim_browser = df_enriched.select(
        col("userAgentInfo.browser_key").alias("browser_key"),
        col("userAgentInfo.browser_family").alias("browser_family"),
        col("userAgentInfo.browser_version").alias("browser_version")
    )
    dim_os = df_enriched.select(
        col("userAgentInfo.os_key").alias("os_key"),
        col("userAgentInfo.os_family").alias("os_family"),
        col("userAgentInfo.os_version").alias("os_version")
    )
    dim_tld = df_enriched.select(
        col("tldInfo.tld").alias("tld"),
        col("tldInfo.country_name").alias("country_name")
    )

    # Extract fact table
    fact_event = df_enriched.select(
        col("jsonData.id").alias("id"),
        col("jsonData.time_stamp").alias("time_stamp"),
        col("jsonData.ip").alias("ip"),
        col("jsonData.device_id").alias("device_id"),
        col("jsonData.api_version").alias("api_version"),
        col("jsonData.store_id").alias("store_id"),
        col("jsonData.local_time").alias("local_time"),
        col("jsonData.current_url").alias("current_url"),
        col("jsonData.referrer_url").alias("referrer_url"),
        col("jsonData.email").alias("email"),
        col("jsonData.collection").alias("collection"),
        col("jsonData.product_id").alias("product_id"),
        col("jsonData.option").alias("option"),
        col("userAgentInfo.browser_key").alias("browser_key"),
        col("userAgentInfo.os_key").alias("os_key"),
        col("tldInfo.tld").alias("tld"),
        to_date(col("jsonData.local_time")).alias("date_key")
    )

    dim_browser_query = dim_browser.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: upsert_dim_table(df, id, "dim_browser", ["browser_key"])) \
        .start()

    dim_os_query = dim_os.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: upsert_dim_table(df, id, "dim_os", ["os_key"])) \
        .start()

    dim_tld_query = dim_tld.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: upsert_dim_table(df, id, "dim_tld", ["tld"])) \
        .start()

    fact_event_query = fact_event.writeStream \
        .outputMode("append") \
        .foreachBatch(write_fact_table) \
        .start()

    spark.streams.awaitAnyTermination()
