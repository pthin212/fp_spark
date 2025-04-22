import tldextract
import pycountry
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField


def get_tld(url):
    extracted = tldextract.extract(url)
    tld_suffix = extracted.suffix

    tld_parts = tld_suffix.lower().split(".")
    tld_end = tld_parts[-1]

    if tld_end == "uk":
        return tld_end, "United Kingdom"
    elif tld_end == "tr":
        return tld_end, "Turkey"
    else:
        try:
            country = pycountry.countries.get(alpha_2=tld_end.upper())
            return tld_end, country.name
        except Exception as e:
            return tld_end, None


def register_url_udfs(spark):
    get_tld_udf = udf(get_tld, StructType([
        StructField("tld", StringType(), True),
        StructField("country_name", StringType(), True)
    ]))

    spark.udf.register("get_tld", get_tld_udf)

    return get_tld_udf