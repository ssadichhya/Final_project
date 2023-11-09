import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re
import yaml
import logger_file as lf
from clean import clean
from pathlib import Path
import os


logger = lf.setup_logs()
logger.info(f"Logger initialized successfully for transform file!!")
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
logger.info(f"Spark Initialized Successfully for transform file!!!")


# %%
def transform():
    try:
        def calculate_average(range_str):
            # Using regular expression to extract numbers
            numbers = re.findall(r'\d+', range_str)
            if len(numbers) == 2:
                lower = int(numbers[0])
                upper = int(numbers[1])
                avg = (lower + upper) / 2
                return avg
            else:
                return None

        df=clean(spark)
        calculate_average_udf = F.udf(calculate_average)
        # Add a new column with the calculated average
        new_df = df.withColumn("Average", calculate_average_udf(df["Salary Range"]))
        new_df = new_df.withColumnRenamed("Average","Average Salary")
        new_df = new_df.withColumn("Average Salary",  
                                  new_df["Average Salary"] 
                                  .cast('int')) 

        
        # Changing gender preference from "both" to "Male or Female"
        new_df = new_df.withColumn("Preference", F.when(F.col("Preference") == "Both", "Male or Female").otherwise(F.col("Preference")))

        #Dividing companies into tiers according to the company size

        quartiles = new_df.stat.approxQuantile("company size", [0.25, 0.75], 0.0)
        q1, q3 = quartiles
        new_df = new_df.withColumn("CompanyTier",
        F.when(F.col("company size") <= q1, "Tier-1 (Low)")
        .when((F.col("company size") > q1) & (F.col("company size") <= q3), "Tier-2 (Medium)")
        .when(F.col("company size") > q3, "Tier-3 (High)")
        .otherwise("Uncategorized"))


        #Dividing jobs into three categories according to their salary.
        
        quartiles = new_df.stat.approxQuantile("Average Salary", [0.25, 0.75], 0.0)
        q1, q3 = quartiles
        new_df = new_df.withColumn("SalaryLevel",
        F.when(F.col("Average Salary") <= q1, "Low Pay")
        .when((F.col("Average Salary") > q1) & (F.col("Average Salary") <= q3), "Average Pay")
        .when(F.col("Average Salary") > q3, "High Pay")
        .otherwise("Uncategorized"))
        

        # Differentiate qualifications according to their initials

        new_df = new_df.withColumn("QualificationCategory", F.when(F.col("qualifications").startswith("M"), "Masters")
        .when(F.col("qualifications").startswith("B"), "Bachelors")
        .when(F.col("qualifications").startswith("P"), "PhD")
        .otherwise("Uncategorized"))

        logger.info(f"Data transformed successfully!!!!")
        return new_df

    except Exception as e:
        logger.error(f"An error occurred during data transformation: {str(e)}")
        spark.stop()

