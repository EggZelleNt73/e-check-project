from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col
from biedronka_e_check_code.parse_json_b_check import run_biedronka_execution
from lidl_e_check_code.parse_csv_lidl_check import run_lidl_execution
from load_files import download_files_func
import os, sys
from utils.logger_setup import get_logger
from gs_load import load_to_google_sheet

CSV_SOURCE_DIR = "/opt/spark/source_data/csv_files"
JSON_SOURCE_DIR = "/opt/spark/source_data/json_files"
SINK_DIR = "/opt/spark/sink_data/csv_file"
CATEGORY_FILE_LOC = "/opt/spark/code_exe/category_type.json"

# Create logger
logger = get_logger(__name__)

# Downloading files from google drive
logger.info("Connecting to google drive")
# download_files_func()

# Checking for file existence in source directories
def has_files(directory, extension):
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exis: {os.path.exists(directory)}")
        return False
    for fname in os.listdir(directory):
        if fname.startswith("."):
            continue
        if fname.lower().endswith(extension):
            return True
    logger.warning(f"Couldn't locate {extension} in {directory}")
    return False

has_csv = has_files(CSV_SOURCE_DIR, (".csv",))
has_json = has_files(JSON_SOURCE_DIR, (".json",))

if not has_csv and not has_json:
    logger.info("No files found in source directory. Exiting...")
    sys.exit(0)

logger.info("Starting Spark session")
    
# Creating spark session
spark = SparkSession.builder.appName("E-check transformator").getOrCreate()

# Reading biedroka e-check files
def save_read_json(path: str):
    """Try to read JSON file safely — return None on failure."""
    try:
        logger.info("Reading json files...")
        df_bied = spark.read.option("multiline", True).json(path)
        if df_bied.isEmpty() or not df_bied.columns or "_corrupt_record" in df_bied.columns:
            logger.warning("Not able to load json files into dataframe")
            return None
        logger.info("JSON files read successfully")
        return df_bied
    except Exception as e:
        logger.error(f"Failed to read json files: {e}")
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
        logger.info("Reading csv files...")
        df_lidl = spark.read.csv(path, header=True, schema=schema)
        if df_lidl.isEmpty() or not df_lidl.columns or "_corrupt_record" in df_lidl.columns:
            logger.warning("Not able to load csv files into dataframe")
            return None
        logger.info("CSV files read successfully")
        return df_lidl
    except Exception as e:
        logger.error(f"Failed to read json files: {e}")
        return None

df_lidl = None
df_bied = None

# Reading files and creating data frames
if has_json:
    df_bied = save_read_json(JSON_SOURCE_DIR)

if has_csv:
    df_lidl = save_read_csv(CSV_SOURCE_DIR)

if df_bied is None and df_lidl is None:
    logger.info("No data available. Exiting...")
    spark.stop()
    sys.exit(0)

# Reading json file with categories and types to map
df_json = spark.read.option("multiline", True).json(CATEGORY_FILE_LOC)
df_json.cache()

# Execute biedronka transformation
if df_bied is None:
    logger.info("Skipping Biedronka transformation - invalid or missing data")
else:
    df_bied = run_biedronka_execution(df_bied, df_json)

# Execute lidl transformation
if df_lidl is None:
    logger.info("Skipping Lidl transformation - invalid or missing data")
else:
    df_lidl = run_lidl_execution(df_lidl, df_json)

logger.info("Creating final data frame")
# Final adjustments
try:
    if df_bied and df_lidl:
        df_final = df_bied.union(df_lidl)
    elif df_bied:
        df_final = df_bied
    else:
        df_final = df_lidl
    logger.info("Final data frame created")
except Exception as e:
    logger.error(f"Failed to create final data frame: {e}")

df_final = df_final.orderBy(col("date").asc())

logger.info("Saving data frame into sink directory")
# Saving data frame as csv file
try:
    df_final.coalesce(1).write.mode("overwrite").option("header", True).csv(SINK_DIR)
    logger.info("Successfully saved data frame")
except Exception as e:
    logger.error(f"Failed to save dataframe: {e}")

logger.info("Stopping spark session")

spark.stop()

# load_to_google_sheet()

