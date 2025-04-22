import hashlib
from user_agents import parse
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, BooleanType


def hash_string(input_string):
    if input_string is None:
        return None
    encoded_string = input_string.encode('utf-8')
    return hashlib.sha256(encoded_string).hexdigest()


def parse_user_agent(user_agent_string):
    try:
        user_agent = parse(user_agent_string)
        browser_family = user_agent.browser.family
        browser_version = user_agent.browser.version_string
        os_family = user_agent.os.family
        os_version = user_agent.os.version_string

        browser_key = hash_string(browser_family + browser_version)
        os_key = hash_string(os_family + os_version)

        return {
            "browser_family": user_agent.browser.family,
            "browser_version": user_agent.browser.version_string,
            "os_family": user_agent.os.family,
            "os_version": user_agent.os.version_string,
            "browser_key": browser_key,
            "os_key": os_key
        }
    except Exception as e:
        return {
            "browser_family": None,
            "browser_version": None,
            "os_family": None,
            "os_version": None,
            "browser_key": None,
            "os_key": None
        }


def register_user_agent_udfs(spark):
    parse_user_agent_udf = udf(parse_user_agent, StructType([
        StructField("browser_family", StringType(), True),
        StructField("browser_version", StringType(), True),
        StructField("os_family", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("browser_key", StringType(), True),
        StructField("os_key", StringType(), True)
    ]))

    spark.udf.register("parse_user_agent", parse_user_agent_udf)

    return parse_user_agent_udf