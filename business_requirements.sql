-- Top 10 product_id with the highest view count for the current day
SELECT
    product_id,
    COUNT(*) AS view_count
FROM
    fact_event
WHERE
    date_key = CURRENT_DATE AND product_id IS NOT NULL
GROUP BY
    product_id
ORDER BY
    view_count DESC
LIMIT 10;

-- Top 10 countries with the highest view count for the current day (country is determined based on domain)
SELECT
    dt.country_name,
    COUNT(*) AS view_count
FROM
    fact_event fe
LEFT JOIN
    dim_tld dt ON fe.tld = dt.tld
WHERE
    fe.date_key = CURRENT_DATE
    AND dt.country_name IS NOT NULL
GROUP BY
    dt.country_name
ORDER BY
    view_count DESC
LIMIT 10;

-- Top 5 referrer_url with the highest view count for the current day
SELECT
    referrer_url,
    COUNT(*) AS view_count
FROM
    fact_event
WHERE
    date_key = CURRENT_DATE
    AND referrer_url IS NOT NULL
    AND referrer_url != ''
GROUP BY
    referrer_url
ORDER BY
    view_count DESC
LIMIT 5;

-- For a given country, retrieve the list of store_id and their corresponding view counts, sorted in descending order of view count
-- Country: Viet Nam
SELECT
    fe.store_id,
    COUNT(*) AS view_count
FROM
    fact_event fe
LEFT JOIN
    dim_tld dt ON fe.tld = dt.tld
WHERE
    dt.country_name = 'Viet Nam'
GROUP BY
    fe.store_id
ORDER BY
    view_count DESC;

-- View data distribution by hour for a given product_id within the day
-- Current date, Product id 90736
SELECT
    date_key,
    EXTRACT(HOUR FROM local_time::timestamp) AS hour_of_day,
    COUNT(*) AS view_count
FROM
    fact_event
WHERE
    date_key = CURRENT_DATE
    AND product_id = '90736'
GROUP BY
    date_key,
    hour_of_day
ORDER BY
    date_key,
    hour_of_day;

-- Hourly view data for each browser and OS
-- Current Date
SELECT
    fe.date_key,
    EXTRACT(HOUR FROM local_time::timestamp) AS hour_of_day,
    db.browser_family,
    dos.os_family,
    COUNT(*) AS view_count
FROM
    fact_event fe
LEFT JOIN
    dim_browser db ON fe.browser_key = db.browser_key
LEFT JOIN
    dim_os dos ON fe.os_key = dos.os_key
WHERE
    fe.date_key = CURRENT_DATE
GROUP BY
    fe.date_key,
    hour_of_day,
    db.browser_family,
    dos.os_family
ORDER BY
    fe.date_key,
    hour_of_day,
    db.browser_family,
    dos.os_family;