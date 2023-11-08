import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, FloatType, IntegerType, DateType
import pyspark.sql.functions as F
import re
import yaml
import logger_file as lf
from extract import extract
from pathlib import Path
import os


logger = lf.setup_logs()
logger.info(f"Logger initialized successfully in clean file!!")
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
logger.info(f"Spark Initialized Successfully for cleaning file!!!")

def clean(spark):
    try:
        logger.info(f"Got inside clean function")
        df = extract(spark)
        df.show(1)
        # Changing -ve company size to positive
        df = df.withColumn("Company Size", F.when(F.col("Company Size") < 0, -F.col("Company Size")).otherwise(F.col("Company Size")))
        
        # List of columns to drop
        dropped = ["Contact Person", "Contact", "Benefits"]
        # Drop columns
        df = df.drop(*dropped)
        df.show(3)
        
        # Changing to standard data types
        df = df.withColumn("Job Id", df["Job Id"].cast(LongType()))\
               .withColumn("latitude", df["latitude"].cast(FloatType()))\
               .withColumn("longitude", df["longitude"].cast(FloatType()))\
               .withColumn("Company Size", df["Company Size"].cast(IntegerType()))\
               .withColumn("Job Posting Date", df["Job Posting Date"].cast(DateType()))
        # sector_pattern = r'"Sector":"([^"]+)"'
        df.show(2)

        # # Use regexp_extract to extract the value of "Sector" into a new column "Sector"
        # df = df.withColumn("Sector", F.regexp_extract(df["Company Profile"], sector_pattern, 1))
        df = df.withColumn("Sector", F.substring_index(F.col("Company Profile"), '":"', -1))
        df = df.withColumn("Sector", F.regexp_replace(F.col("Sector"), '"', ''))
        df = df.drop("Company Profile")
        df.show(1)

        logger.info(f"Data Cleaned Succesfully")
        return df
    except Exception as e:
        logger.error(f"An error occurred during data cleaning: {str(e)}")
        spark.stop()


# dff = clean(spark)
