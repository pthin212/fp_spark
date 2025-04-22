from pyspark.sql.functions import col, to_date


def extract_fact_table(df_enriched):
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

    return fact_event


def write_fact_table(batch_df, batch_id, jdbc_url, jdbc_properties):
    batch_df.write \
        .jdbc(url=jdbc_url,
              table="fact_event",
              mode="append",
              properties=jdbc_properties)