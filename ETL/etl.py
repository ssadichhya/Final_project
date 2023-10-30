# %%
from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, FloatType, IntegerType, DateType
import pyspark.sql.functions as F
import re



# %%
def extract(spark):
        
    #csv path
    csv = "/home/ubuntu/Desktop/final_project/Raw_Data/job_descriptions.csv"
    #Read raw_data
    df = spark.read.csv(csv, header=True, inferSchema=False)
    return df


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
    df=extract(spark)
    #List of columns to drop 
    dropped = ["Contact Person", "Contact", "Benefits","Company Profile"]
    #Drop column
    df = df.drop(*dropped)
    #changing to standard datatype
    df = df.withColumn("Job Id", df["Job Id"].cast(LongType()))\
        .withColumn("latitude", df["latitude"].cast(FloatType()))\
         .withColumn("longitude", df["longitude"].cast(FloatType()))\
          .withColumn("Company Size", df["Company Size"].cast(IntegerType()))\
           .withColumn("Job Posting Date", df["Job Posting Date"].cast(DateType()))
    calculate_average_udf = F.udf(calculate_average)

    # Add a new column with the calculated average
    new_df = df.withColumn("Average", calculate_average_udf(df["Salary Range"]))
    new_df.withColumnRenamed("Average","Average Salary").printSchema()
    return new_df

# %%
def load(spark):
    new_df=transform(spark)
    ##Load the clean data in postgres
    new_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/final_project',driver = 'org.postgresql.Driver', dbtable = 'job_description_clean', user="postgres",password="postgres" ).mode('overwrite').save()
    return new_df

# # %%
# dff=load()
# dff.show(3)




