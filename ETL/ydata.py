import pandas as pd
from ydata_profiling import ProfileReport

csv = "/home/ubuntu/Desktop/final_project/Raw_Data/job_descriptions.csv"

df = pd.read_csv(csv)
profile = ProfileReport(df, title="Profiling Report")
