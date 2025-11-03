from pyspark.sql import SparkSession
from biedronka_e_check_code.parse_json_b_check import run_biedronka_execution

spark = SparkSession.builder.appName("E-check transformator").getOrCreate()

df_json = spark.read.option("multiline", True).json("/opt/spark/code_exe/category_type.json")
df_bied = spark.read.option("multiline", True).json("/opt/spark/source_data/")

# Execute transformations
df_bied = run_biedronka_execution(df_bied, df_json)

df_bied.show()

spark.stop()
