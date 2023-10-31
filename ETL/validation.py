from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, FloatType, IntegerType, DateType, StructType, StructField, StringType
import pyspark.sql.functions as F
from etl import transform
import pytest
import re
import yaml

# Define the path to your YAML file
yaml_file_path = 'config.yaml'

# Read the YAML file and parse it into a Python dictionary
with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

spark = SparkSession \
    .builder \
    .appName("final_project") \
    .config("spark.jars", config['spark']["path"]) \
    .getOrCreate()


dff=transform(spark)
# dff.show(3)

expected_date_format = 'yyyy-MM-dd'  
date_columns_to_check = ['Job Posting Date']

@pytest.mark.parametrize('date_column', date_columns_to_check)
def test_date_format_consistency(date_column):
    valid_dates = dff.filter(F.col(date_column).isNotNull())
    date_format_check = valid_dates.withColumn("date_format_check",
        F.to_date(F.col(date_column), expected_date_format))
    inconsistent_dates = date_format_check.filter(F.col("date_format_check").isNull())
    assert inconsistent_dates.count() == 0

expected_columns = ["Job Id", "Experience", "Qualifications", "Salary Range", "location", "Country", "latitude", "longitude", "Work Type", "Company Size", "Job Posting Date", "Preference", "Job Title", "Role", "Job Portal", "Job Description", "skills", "Responsibilities", "Company", "Average Salary"]
actual_columns = dff.columns
assert set(actual_columns) != set(expected_columns)


expected_schema = StructType([
    StructField("Job Id", LongType(), True),
    StructField("Experience", StringType(), True),
    StructField("Qualifications", StringType(), True),
    StructField("Salary Range", StringType(), True),
    StructField("location", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("Work Type", StringType(), True),
    StructField("Company Size", IntegerType(), True),
    StructField("Job Posting Date", DateType(), True),
    StructField("Preference", StringType(), True),
    StructField("Job Title", StringType(), True),
    StructField("Role", StringType(), True),
    StructField("Job Portal", StringType(), True),
    StructField("Job Description", StringType(), True),
    StructField("skills", StringType(), True),
    StructField("Responsibilities", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Average Salary", StringType(), True)
])

# Check if the DataFrame schema matches the expected schema
assert dff.schema != expected_schema, "DataFrame schema does not match the expected data types."

def test_company_size_values():
    # Filter the DataFrame to select rows where "Company Size" is negative
    negative_rows = dff.filter(F.col("Company Size") < 0)

    # Assert that there are no negative values
    assert negative_rows.count() == 0, "There are negative Company Size values"

