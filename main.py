from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from udfs.user_agent_udfs import register_user_agent_udfs
from udfs.url_udfs import register_url_udfs

from tables.dim_tables import upsert_dim_table, extract_browser_dim, extract_os_dim, extract_tld_dim
from tables.fact_tables import extract_fact_table, write_fact_table
from tables.process_batch import process_batch

from util.config import Config
from util.logger import Log4j

from schema.event_schema import create_json_schema

if __name__ == "__main__":

    conf = Config()
    spark_conf = conf.spark_conf
    kafka_conf = conf.kafka_conf
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
    log.info(f"kafka_conf: {kafka_conf.items()}")

    # Register UDFs
    parse_user_agent_udf = register_user_agent_udfs(spark)
    get_tld_udf = register_url_udfs(spark)

    # Read stream from Kafka
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_conf) \
        .load()


    # Parse JSON data
    json_schema = create_json_schema()
    df_parsed = df.select(from_json(col("value").cast("string"), json_schema).alias("jsonData"))

    # Enrich data with user agent, TLD information
    df_enriched = df_parsed.withColumn("userAgentInfo", parse_user_agent_udf(col("jsonData.user_agent"))) \
        .withColumn("tldInfo", get_tld_udf(col("jsonData.current_url")))


    # Start streaming query to process each batch and write to dim/fact tables via process_batch function
    query = df_enriched.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: process_batch(df, id, spark, jdbc_url, jdbc_properties, extract_browser_dim, extract_os_dim, extract_tld_dim, extract_fact_table, upsert_dim_table, write_fact_table)) \
        .start()

    spark.streams.awaitAnyTermination()