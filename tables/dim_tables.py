from pyspark.sql.functions import col


def extract_browser_dim(df_enriched):
    return df_enriched.select(
        col("userAgentInfo.browser_key").alias("browser_key"),
        col("userAgentInfo.browser_family").alias("browser_family"),
        col("userAgentInfo.browser_version").alias("browser_version")
    ).dropDuplicates(["browser_key"])


def extract_os_dim(df_enriched):
    return df_enriched.select(
        col("userAgentInfo.os_key").alias("os_key"),
        col("userAgentInfo.os_family").alias("os_family"),
        col("userAgentInfo.os_version").alias("os_version")
    ).dropDuplicates(["os_key"])


def extract_tld_dim(df_enriched):
    return df_enriched.select(
        col("tldInfo.tld").alias("tld"),
        col("tldInfo.country_name").alias("country_name")
    ).dropDuplicates(["tld"])


def upsert_dim_table(batch_df, batch_id, table_name, key_columns, spark, jdbc_url, jdbc_properties):
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