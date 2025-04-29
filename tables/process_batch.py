def process_batch(batch_df, batch_id, spark, jdbc_url, jdbc_properties, extract_browser_dim, extract_os_dim, extract_tld_dim, extract_fact_table, upsert_dim_table, write_fact_table):
    # Extract dimension, fact tables
    dim_browser = extract_browser_dim(batch_df)
    dim_os = extract_os_dim(batch_df)
    dim_tld = extract_tld_dim(batch_df)
    fact_event = extract_fact_table(batch_df)

    # Write tables
    upsert_dim_table(dim_browser, batch_id, "dim_browser", ["browser_key"], spark, jdbc_url, jdbc_properties)
    upsert_dim_table(dim_os, batch_id, "dim_os", ["os_key"], spark, jdbc_url, jdbc_properties)
    upsert_dim_table(dim_tld, batch_id, "dim_tld", ["tld"], spark, jdbc_url, jdbc_properties)
    write_fact_table(fact_event, batch_id, jdbc_url, jdbc_properties)