import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import yaml
import logger_file as lf
import os
from pathlib import Path

logger = lf.setup_logs()
logger.info(f"Logger initialized successfully for extract file!!")
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
logger.info(f"Spark Initialized Successfully for extract file!!!")

# %%
def extract(spark):
    try:
        # CSV path
        csv = config['csv']['path']
        # Read raw_data
        df = spark.read.csv(csv, header=True, inferSchema=True)
        logger.info(f"Data Extracted Successfully!!!")
        return df
    
    except Exception as e:
        logger.error(f"An error occurred during data extraction: {str(e)}")
        spark.stop()