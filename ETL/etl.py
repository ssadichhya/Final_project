# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, FloatType, IntegerType, DateType, MapType
import pyspark.sql.functions as F
import re
import yaml

#define path to your yaml file
yaml_file_path= 'config.yaml'

with open(yaml_file_path, 'r') as yaml_file:
    config = yaml.safe_load(yaml_file)

spark = SparkSession \
    .builder \
    .appName("final_project") \
    .config("spark.jars", config['spark']['path']) \
    .getOrCreate()

# %%
def extract(spark):
    try:
        # CSV path
        csv = config['csv']['path']
        # Read raw_data
        df = spark.read.csv(csv, header=True, inferSchema=False)
        return df
    except Exception as e:
        raise Exception(f"An error occurred during data extraction: {str(e)}")
        spark.stop()

# %%
df=extract(spark)
df.select("Company Profile").show(truncate=False)

# %%
def clean(spark):
    try:
        df = extract(spark)
        # Changing -ve company size to positive
        df = df.withColumn("Company Size", F.when(F.col("Company Size") < 0, -F.col("Company Size")).otherwise(F.col("Company Size")))
        
        # List of columns to drop
        dropped = ["Contact Person", "Contact", "Benefits"]
        # Drop columns
        df = df.drop(*dropped)
        
        # Changing to standard data types
        df = df.withColumn("Job Id", df["Job Id"].cast(LongType()))\
               .withColumn("latitude", df["latitude"].cast(FloatType()))\
               .withColumn("longitude", df["longitude"].cast(FloatType()))\
               .withColumn("Company Size", df["Company Size"].cast(IntegerType()))\
               .withColumn("Job Posting Date", df["Job Posting Date"].cast(DateType()))
        
        # sector_pattern = r'"Sector":"([^"]+)"'

        # # Use regexp_extract to extract the value of "Sector" into a new column "Sector"
        # df = df.withColumn("Sector", F.regexp_extract(df["Company Profile"], sector_pattern, 1))
        df = df.withColumn("Sector", F.substring_index(F.col("Company Profile"), '":"', -1))
        df = df.withColumn("Sector", F.regexp_replace(F.col("Sector"), '"', ''))
        df=df.drop("Company Profile")




        return df
    except Exception as e:
        raise Exception(f"An error occurred during data cleaning: {str(e)}")
        spark.stop()



# %%
#udf to calculate avg
def calculate_average(range_str):
    # Use regular expression to extract numbers
    numbers = re.findall(r'\d+', range_str)
    if len(numbers) == 2:
        lower = int(numbers[0])
        upper = int(numbers[1])
        avg = (lower + upper) / 2
        return avg
    else:
        return None


# %%
def transform(spark):
    try:
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
        new_df.show()

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


        return new_df

    except Exception as e:
        raise Exception(f"An error occurred during data transformation: {str(e)}")
        spark.stop()



# %%
def load(spark):
    try:
        dff=transform(spark)
        ##Load the clean data in postgres
        dff.write.format('jdbc').options(url=config['postgres']["url"],driver = config['postgres']["driver"], dbtable = config['postgres']["dbtable"], user=config['postgres']["user"],password=config['postgres']["password"]).mode('overwrite').save()
        return dff
    except Exception as e:
        raise Exception(f"An error occurred during loading the data: {str(e)}")
        spark.stop()    




