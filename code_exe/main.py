from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from biedronka_e_check_code.parse_json_b_check import run_biedronka_execution
from lidl_e_check_code.parse_csv_lidl_check import run_lidl_execution
from load_files import download_files_func
import time, threading, os, sys

SOURCE_DIR = "/opt/spark/source_data"

# Downloading files from google drive
download_files_func()

files = [f for f in os.listdir(SOURCE_DIR) if not f.startswith(".")]

if not files:
    print("No files found in source directory. Exiting..")
    sys.exit(0)

print("Start spark execution")
    
# Creating spark session
spark = SparkSession.builder.appName("E-check transformator").getOrCreate()

# Reading biedroka e-check files
def save_read_json(path: str):
    """Try to read JSON file safely — return None on failure."""
    try:
        df_bied = spark.read.option("multiline", True).json(path)
        if df_bied.isEmpty() or not df_bied.columns or "_corrupt_record" in df_bied.columns:
            print("JSON source is empty")
            return None
        print("JSON file loaded")
        return df_bied
    except Exception as e:
        print("Failed to read JSON files")
        return None


# Reading lidl e-check files
def save_read_csv(path: str):
    """Try to read CSV file safely — return None on failure."""
    schema = StructType([
            StructField("item_name", StringType(), True),
            StructField("quantity", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("discount_value", DoubleType(), True),
            StructField("discount", StringType(), True),
            StructField("time", StringType(), True),
            StructField("date", StringType(), True)
        ])

    try:
        df_lidl = spark.read.csv(path, header=True, schema=schema)
        if df_lidl.isEmpty() or not df_lidl.columns or "_corrupt_record" in df_lidl.columns:
            print("JSON source is empty")
            return None
        return df_lidl
    except Exception as e:
        print("Failed to read CSV files")
        return None

df_bied = save_read_json(SOURCE_DIR)
df_lidl = save_read_csv(SOURCE_DIR)

if df_bied is None and df_lidl is None:
    print("No data available. Exiting...")
    spark.stop()
    sys.exit(0)

# Reading json file with categories and types to map
df_json = spark.read.option("multiline", True).json("/opt/spark/code_exe/category_type.json")
df_json.cache()

# Execute biedronka transformation
if df_bied is None:
    print("Skipping Biedronka transformation - invalid or missing data")
else:
    df_bied = run_biedronka_execution(df_bied, df_json)

# Execute lidl transformation
if df_lidl is None:
    print("Skipping Lidl transformation - invalid or missing data")
else:
    df_lidl = run_lidl_execution(df_lidl, df_json)

# Final adjustments
if df_bied and df_lidl:
    df_final = df_bied.union(df_lidl)
elif df_bied:
    df_final = df_bied
else:
    df_final = df_lidl

df_final = df_final.orderBy(col("date").asc())

df_final.coalesce(1).write.mode("overwrite").option("header", True).csv("/opt/spark/sink_data/csv_file")

spark.stop()
