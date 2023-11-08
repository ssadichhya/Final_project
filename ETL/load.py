import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import yaml
import logger_file as lf
from transform import transform
from pathlib import Path
import os

logger = lf.setup_logs()
logger.info(f"Logger initialized successfully in clean!!")
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
logger.info(f"Spark Initialized Successfully!!!")

def load():
    try:
        dff=transform()
        ##Load the clean data in postgres
        dff.write.format('jdbc').options(url=config['postgres']["url"],driver = config['postgres']["driver"], dbtable = config['postgres']["dbtable"], user=config['postgres']["user"],password=config['postgres']["password"]).mode('overwrite').save()
        logger.info(f"Data loaded into Postgres Succesfully!!!")
        return dff
    except Exception as e:
        logger.error(f"An error occurred during loading the data: {str(e)}")
        spark.stop() 

load()