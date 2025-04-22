#!/bin/bash

# Stop and remove existing container
docker container stop fp-spark || true
docker container rm fp-spark || true

# Create a zip file of the project for distribution to workers
rm -f 99-project/project.zip
rm -f 99-project/pyspark_venv.tar.gz
rm -rf pyspark_venv
cd 99-project
zip -r project.zip main.py udfs/ tables/ util/
cd ..

# Run the Spark container
docker run -ti --name fp-spark \
  --network=streaming-network \
  --env-file ./99-project/.env \
  -p 4040:4040 \
  -v ./:/spark \
  -v spark_lib:/opt/bitnami/spark/.ivy2 \
  -e PYSPARK_DRIVER_PYTHON='python' \
  -e PYSPARK_PYTHON='./environment/bin/python' \
  unigap/spark:3.5 bash -c "cd /spark/99-project &&
    python -m venv pyspark_venv &&
    source pyspark_venv/bin/activate &&
    pip install -r /spark/requirements.txt &&
    venv-pack -o pyspark_venv.tar.gz &&
    spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 \
      --py-files project.zip \
      --archives pyspark_venv.tar.gz#environment \
      main.py"