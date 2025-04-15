import os
from pyspark import SparkConf

class Config:
    def __init__(self):
        self.spark_conf = self._get_spark_conf()
        self.kafka_conf = self._get_kafka_conf()
        self.postgres_conf = self._get_postgres_conf()

    def _get_spark_conf(self):
        conf = SparkConf()
        conf.set("spark.app.name", os.getenv("SPARK_APP_NAME"))
        conf.set("spark.master", os.getenv("SPARK_MASTER"))
        return conf

    def _get_kafka_conf(self):
        return {
            "kafka.bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
            "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL"),
            "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
            "kafka.sasl.jaas.config": os.getenv("KAFKA_SASL_JAAS_CONFIG"),
            "subscribe": os.getenv("KAFKA_SUBSCRIBE")
        }

    def _get_postgres_conf(self):
        return {
            "url": os.getenv("POSTGRES_JDBC_URL"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "driver": "org.postgresql.Driver"
        }