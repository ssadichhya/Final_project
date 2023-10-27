from etl import load 
import pandas as pd


dff=load()
dff.head()

# # Define a function to check date format consistency
# def check_date_format_consistency(date_column):
#     date_format_udf = udf(lambda x: x.strftime('yyyy-MM-dd'), StringType())
#     df_with_common_format = df.withColumn(date_column, date_format_udf(col(date_column)))
#     unique_formats = df_with_common_format.select(date_column).distinct().count()
#     return unique_formats

# # List of date-related columns
# date_columns = ['Job Posting Date', 'AnotherDateColumn']  # Add other date columns as needed

# # Pytest test function
# @pytest.mark.parametrize("column", date_columns)
# def test_date_format_consistency(column):
#     unique_formats_count = check_date_format_consistency(column)
#     assert unique_formats_count == 1, f"Date format in '{column}' is not consistent. Found {unique_formats_count} unique formats."
