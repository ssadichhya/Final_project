import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType, DateType, StructType, StructField, StringType
import re
import yaml
import logger_file as lf
import pytest
from extract import extract
from pathlib import Path
import os


logger = lf.setup_logs()
logger.info(f"Logger initialized successfully for validation file!!")
basedir = Path(__file__).parents[0]
#define path to yaml file
yaml_file_path= os.path.join(basedir, 'config.yaml')

with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

spark = SparkSession \
    .builder \
    .appName("final_project") \
    .config("spark.jars", config['spark']['path']) \
    .getOrCreate()
logger.info(f"Spark Initialized Successfully for validation file!!!")


dff=extract(spark)
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

def test_column_name_values():
    expected_columns = ["Job Id", "experience", "qualifications", "Salary Range", "location", "country", "latitude", "longitude", "Work Type", "Company Size", "Job Posting Date", "preference","Contact Person", "contact",  "Job Title", "Role", "Job Portal", "Job Description","benefits", "skills", "responsibilities", "company", "Company Profile"]
    actual_columns = dff.columns
    assert set(actual_columns) == set(expected_columns)

def test_dataset_schema_values():
    expected_schema = StructType([
        StructField("Job Id", LongType(), True),
        StructField("experience", StringType(), True),
        StructField("qualifications", StringType(), True),
        StructField("Salary Range", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("Work Type", StringType(), True),
        StructField("Company Size", IntegerType(), True),
        StructField("Job Posting Date", DateType(), True),
        StructField("preference", StringType(), True),
        StructField("Contact Person", StringType(), True),
        StructField("contact", StringType(), True),
        StructField("Job Title", StringType(), True),
        StructField("Role", StringType(), True),
        StructField("Job Portal", StringType(), True),
        StructField("Job Description", StringType(), True),
        StructField("benefits", StringType(), True),
        StructField("skills", StringType(), True),
        StructField("responsibilities", StringType(), True),
        StructField("company", StringType(), True),
        StructField("Company Profile", StringType(), True)
    ])

        # Check if the DataFrame schema matches the expected schema
    assert dff.schema == expected_schema, "DataFrame schema does not match the expected data types."
    logger.info(f"Validation passed successfully")