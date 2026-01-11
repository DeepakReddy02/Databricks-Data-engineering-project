%pip install python-dotenv
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import max
from datetime import datetime
import pandas as pd
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

metric_struct = StructType([
    StructField("avg", DoubleType(), True),
    StructField("max", DoubleType(), True),
    StructField("min", DoubleType(), True),
    StructField("sum", DoubleType(), True),
    StructField("count", IntegerType(), True),
    StructField("stddev", DoubleType(), True),
    StructField("LandAreaWeight", DoubleType(), True),
    StructField("TotalPopulationWeight", DoubleType(), True),
    StructField("NumberOfHouseholdsWeight", DoubleType(), True),
    StructField("TotalPopulationMaleWeight", DoubleType(), True),
    StructField("TotalPopulationFemaleWeight", DoubleType(), True)
])

# Full schema
Data_schema = StructType([
    StructField("Country", StringType(), True),
    StructField("StateName", StringType(), True),
    StructField("StateCode", IntegerType(), True),
    StructField("DistrictName", StringType(), True),
    StructField("DistrictCode", IntegerType(), True),
    StructField("Year", StringType(), True),
    StructField("CalendarDay", StringType(), True),
    StructField("D7319_1", StringType(), True),
    StructField("D7319_9", StringType(), True),
    StructField("D7319_10", StringType(), True),
    StructField("D7319_14", StringType(), True),
    StructField("D7319_15", StringType(), True),
    StructField("D7319_19", StringType(), True),
    StructField("D7319_20", StringType(), True),
    StructField("D7319_24", StringType(), True),

    StructField("I7319_6", metric_struct, True),
    StructField("I7319_7", metric_struct, True),
    StructField("I7319_8", metric_struct, True),
    StructField("I7319_11", metric_struct, True),
    StructField("I7319_12", metric_struct, True),
    StructField("I7319_13", metric_struct, True),
    StructField("I7319_16", metric_struct, True),
    StructField("I7319_17", metric_struct, True),
    StructField("I7319_18", metric_struct, True),
    StructField("I7319_21", metric_struct, True),
    StructField("I7319_22", metric_struct, True),
    StructField("I7319_23", metric_struct, True)
])

load_dotenv()

page = 1
base_url = os.getenv("URL")
if base_url is not None:
    API_KEY = os.getenv("API_KEY")
    DIM_COL = os.getenv("Dim_Col")
    FACT_COL = os.getenv("Fact_Col")
    
else:
    raise ValueError("URL is not set in the environment variables.")

parameters = {
    "API_Key":API_KEY,
    "ind":FACT_COL,
    "dim":DIM_COL
}
page_data = []
header_data = []
Prev_Bronze = spark.read.parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")
max_CalDate_row = Prev_Bronze.select(max("CalendarDay")).collect()[0]
max_CalDate = max_CalDate_row[0] if max_CalDate_row[0] is not None else None
CurrentDate = datetime.now().date()
Default_Dif = CurrentDate - pd.DateOffset(months=1)
if max_CalDate:
    start_date = datetime.strptime(max_CalDate, "%Y-%m-%d").date()
else:
    start_date = (CurrentDate - pd.DateOffset(months=1)).date()
lock = Lock()

def fetch_page(page):
    response = requests.get(base_url + f"?pageno={page}", params=parameters)

    if response.status_code != 200:
        return [], []

    data = response.json()
    Header_API_Data = data.get("Headers", {}).get("Items", [])
    Page_API_Data = data.get("Data", [])

    filtered_rows = []
    filtered_headers = []

    for row in Page_API_Data:
        cd = row.get("CalendarDay")
        if cd:
            cd_date = datetime.strptime(cd, "%Y-%m-%d").date()
            if start_date <= cd_date <= CurrentDate:
                print("Included")
                filtered_rows.append(row)
                filtered_headers.extend(Header_API_Data)

    return filtered_rows, filtered_headers


print("Extraction Start")

page = 1
batch_size = 10
max_workers = 10

while True:
    pages = list(range(page, page + batch_size))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_page, p): p for p in pages}

        has_data = False
        for future in as_completed(futures):
            rows, headers = future.result()

            if rows:
                has_data = True
                with lock:
                    page_data.extend(rows)
                    header_data.extend(headers)

    if not has_data:
        print("No more pages.")
        break

    page += batch_size
    print(page)

print("Extraction End")

print("Extraction End")

spark.createDataFrame(page_data,schema=Data_schema).write.mode("append").parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")
spark.createDataFrame(header_data).write.mode("append").parquet("/Volumes/workspace/rainfall_data/bronze_layer/RainFall_data.parquet")

print("Data written to parquet")