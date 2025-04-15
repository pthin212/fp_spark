# 1. Database Design

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
spark % docker container stop fp-spark || true &&
docker container rm fp-spark || true &&
docker run -ti --name fp-spark \
--network=streaming-network \
--env-file 99-project/.env \
-p 4040:4040 \
-v ./:/spark \
-v spark_lib:/opt/bitnami/spark/.ivy2 \
-e PYSPARK_DRIVER_PYTHON='python' \
-e PYSPARK_PYTHON='./environment/bin/python' \
unigap/spark:3.5 bash -c "python -m venv pyspark_venv &&
source pyspark_venv/bin/activate &&
pip install -r /spark/requirements.txt &&
venv-pack -o pyspark_venv.tar.gz &&
spark-submit \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
--archives pyspark_venv.tar.gz#environment \
/spark/99-project/fp_spark.py"
```
