from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from etl import clean
import pytest



spark = SparkSession \
    .builder \
    .appName("final_project") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g")\
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar") \
    .getOrCreate()


dff=clean(spark)



expected_date_format = 'yyyy-MM-dd'  
date_columns_to_check = ['Job Posting Date']

@pytest.mark.parametrize('date_column', date_columns_to_check)
def test_date_format_consistency(date_column):
    valid_dates = dff.filter(F.col(date_column).isNotNull())
    date_format_check = valid_dates.withColumn("date_format_check",
        F.to_date(F.col(date_column), expected_date_format))
    inconsistent_dates = date_format_check.filter(F.col("date_format_check").isNull())
    assert inconsistent_dates.count() == 0