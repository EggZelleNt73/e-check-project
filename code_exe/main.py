from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from biedronka_e_check_code.parse_json_b_check import run_biedronka_execution
from lidl_e_check_code.parse_csv_lidl_check import run_lidl_execution
from load_files import download_files_func
import time, threading

# download_files_func()

spark = SparkSession.builder.appName("E-check transformator").getOrCreate()

# Reading biedroka e-check files
df_bied = spark.read.option("multiline", True).json("/opt/spark/source_data/")

# Reading lidl e-check files
schema = StructType([
        StructField("item_name", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_value", DoubleType(), True),
        StructField("discount", StringType(), True),
        StructField("time", StringType(), True),
        StructField("date", StringType(), True)
    ])


df_lidl = spark.read.csv("/opt/spark/source_data/", header=True, schema=schema)

# Reading json file with categories and types to map
df_json = spark.read.option("multiline", True).json("/opt/spark/code_exe/category_type.json")
df_json.cache()

# Execute transformations
df_bied = run_biedronka_execution(df_bied, df_json)
df_lidl = run_lidl_execution(df_lidl, df_json)

df_final = df_bied.union(df_lidl)
df_final = df_final.orderBy(col("date").asc())

df_final.coalesce(1).write.mode("overwrite").option("header", True).csv("/opt/spark/sink_data/csv_file")

spark.stop()
