#!/bin/bash

# Stop and remove existing container
docker container stop fp-spark || true
docker container rm fp-spark || true

# Run the Spark container
docker run -ti --name fp-spark \
  --network=streaming-network \
  --env-file ./99-project/.env \
  -p 4040:4040 \
  -v ./:/spark \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -e PYSPARK_DRIVER_PYTHON='python' \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  unigap/spark:3.5 bash -c "
    python -m venv pyspark_venv &&
    source pyspark_venv/bin/activate &&
    pip install -r /spark/requirements.txt &&
    venv-pack -o pyspark_venv.tar.gz &&
    tar -czvf project.tar.gz -C /spark/99-project main.py udfs/ tables/ util/
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --py-files project.tar.gz \
      --archives pyspark_venv.tar.gz#environment \
      /spark/99-project/main.py"