# 1. Dim Model

## 1.1. fact_event
| Column        | Type     |
|---------------|----------|
| `id`          | `text`   |
| `time_stamp`  | `bigint` |
| `ip`          | `text`   |
| `device_id`   | `text`   |
| `api_version` | `text`   |
| `store_id`    | `text`   |
| `local_time`  | `text`   |
| `current_url` | `text`   |
| `referrer_url`| `text`   |
| `email`       | `text`   |
| `collection`  | `text`   |
| `product_id`  | `text`   |
| `option`      | `text`   |
| `browser_key` | `text`   |
| `os_key`      | `text`   |
| `tld`         | `text`   |
| `date_key`    | `date`   |

## 1.2. dim_tld
| Column        | Type   |
|---------------|--------|
| `tld`         | `text` |
| `country_name`| `text` |

## 1.3. dim_os
| Column     | Type   |
|------------|--------|
| `os_key`   | `text` |
| `os_family`| `text` |
| `os_version`| `text` |

## 1.4. dim_date
| Column               | Type    |
|----------------------|---------|
| `date_key`           | `date`  |
| `full_date`          | `text`  |
| `day_of_week`        | `text`  |
| `day_of_week_short`  | `text`  |
| `year_month`         | `text`  |
| `month`              | `text`  |
| `year`               | `text`  |
| `year_number`        | `integer`|
| `is_weekday_or_weekend`| `text`  |

## 1.5. dim_browser
| Column          | Type   |
|-----------------|--------|
| `browser_key`   | `text` |
| `browser_family`| `text` |
| `browser_version`| `text` |

# 2. Run Command
```shell
99-project/run_spark.sh
```
