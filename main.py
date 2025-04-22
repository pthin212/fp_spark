from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json

from udfs.user_agent_udfs import register_user_agent_udfs
from udfs.url_udfs import register_url_udfs

from tables.dim_tables import upsert_dim_table, extract_browser_dim, extract_os_dim, extract_tld_dim
from tables.fact_tables import extract_fact_table, write_fact_table

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

    # Initialize Spark session
    spark = SparkSession.builder \
        .config(conf=spark_conf) \
        .getOrCreate()

    # Set up logging
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

    # Extract dimension tables
    dim_browser = extract_browser_dim(df_enriched)
    dim_os = extract_os_dim(df_enriched)
    dim_tld = extract_tld_dim(df_enriched)

    # Extract fact table
    fact_event = extract_fact_table(df_enriched)

    # Write dimension tables
    dim_browser_query = dim_browser.writeStream \
        .outputMode("append") \
        .foreachBatch(
        lambda df, id: upsert_dim_table(df, id, "dim_browser", ["browser_key"], spark, jdbc_url, jdbc_properties)) \
        .start()

    dim_os_query = dim_os.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: upsert_dim_table(df, id, "dim_os", ["os_key"], spark, jdbc_url, jdbc_properties)) \
        .start()

    dim_tld_query = dim_tld.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: upsert_dim_table(df, id, "dim_tld", ["tld"], spark, jdbc_url, jdbc_properties)) \
        .start()

    # Write fact table
    fact_event_query = fact_event.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda df, id: write_fact_table(df, id, jdbc_url, jdbc_properties)) \
        .start()

    # Wait for any stream to terminate
    spark.streams.awaitAnyTermination()