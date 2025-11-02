from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io, os, glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id, floor, lit, col, when
from category_identification import category_identification_func
from date_transformation import date_transformation_func

# Configuration
SERVICE_ACCOUNT_FILE = glob.glob("/opt/spark/google_auth/e-checks-project*.json")[0]
FOLDER_ID = "1uW77S7QK2xsk-M6urShuGy2HqTpnKykd"
LOCAL_DIR = "/opt/spark/source_data"

# Authenticate
creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive_service = build("drive", "v3", credentials=creds)

# List all CSV files in the folder
query = f"'{FOLDER_ID}' in parents and mimeType='text/csv' and trashed=false"
results = drive_service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    print("No files found")
else:
    # Download files
    for file in files:
        file_id = file["id"]
        file_name = file["name"]
        local_path = os.path.join(LOCAL_DIR, file_name)

        print(f"Downloading: {file_name} ...")

        request = drive_service.files().get_media(fileId=file_id)
        fh = io.FileIO(local_path, "wb")

        downloader = MediaIoBaseDownload(fh, request)
        done = False

        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Progress: {int(status.progress() * 100)}%")
        
        fh.close()
        print(f"Downloaded: {file_name}")
    

    spark = SparkSession.builder.appName("Check_parser_csv_lidl").getOrCreate()

    schema = StructType([
        StructField("item_name", StringType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount_value", DoubleType(), True),
        StructField("discount", StringType(), True),
        StructField("time", StringType(), True),
        StructField("date", StringType(), True)
    ])

    df = spark.read.csv("/opt/spark/source_data/", header=True, schema=schema)
    df = df.withColumn("id", monotonically_increasing_id())
    
    # Creating data frame with category and type namings for mapping
    df_json = spark.read.option("multiline", True).json("/opt/spark/code_exe/category_type.json")
    
    # Category and type mapping 
    df_cat = category_identification_func(df, df_json)
    
    # Date transformation
    df_date = date_transformation_func(df).alias("d")

    df = df.join(df_cat, on="id", how="inner")\
        .join(df_date,  on="id", how="inner")\
        .withColumn("Amount ps", floor(col("quantity")).cast("int"))\
        .withColumn("Amount kg", when(col("quantity") == floor(col("quantity")), lit(0)).otherwise(col("quantity")))\
        .withColumn("Currency", lit("PLN"))\
        .withColumn("Ex rate", lit(1))\
        .withColumn("Amount in PLN", (col("price") * col("Ex rate")))\
        .withColumn("Place", lit("Lidl"))\
        .select(
            col("price").alias("Cost"),
            col("Amount ps"),
            col("Amount kg"),
            col("discount"),
            col("Currency"),
            col("Ex rate"),
            col("Amount in PLN"),
            col("category"),
            col("type"),
            col("d.date"),
            col("time"),
            col("Place")
        )
    
    df = df.orderBy(col("date").asc())
    df = df.fillna(0)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv("/opt/spark/sink_data/csv_file")

    spark.stop()
