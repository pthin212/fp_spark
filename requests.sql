-- Top 10 product_id có lượt view cao nhất trong ngày hiện tại
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

-- Top 10 quốc gia có lượt view cao nhất trong ngày hiện tại (quốc gia được lấy dựa vào domain)
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

-- Top 5 referrer_url có lượt view cao nhất trong ngày hiện tại
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

-- Với 1 quốc gia bất kỳ, lấy ra danh sách các store_id và lượt view tương ứng, sắp xếp theo lượt view giảm dần
-- Quốc gia Việt Nam
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

-- Dữ liệu view phân bổ theo giờ của một product_id bất kỳ trong ngày
-- Ngày hiện tại, Product id 90736
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


-- Dữ liệu view theo giờ của từng browser, os
-- Ngày hiện tại
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