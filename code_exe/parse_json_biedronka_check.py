from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from mass_extraction import mass_extraction_func
from date_extraction import date_time_extration_func
from amount_transformation import amount_to_countable_trans
from category_identification import category_id_func
from gs_load import google_sheet_loader

spark = SparkSession.builder.appName("Check_parser_json_biedronka").getOrCreate()

df = spark.read.option("multiline", True).json("/opt/bitnami/spark/data/")

#=== products data frame creation ===
df_exploded = df.select(explode("body").alias("body"))

# add index to preserve order
df_exploded = df_exploded.withColumn("id", monotonically_increasing_id())\
    .withColumn("filename_id", input_file_name())

# separate products and discounts
trigger = False

if "discountLine" in [field.name for field in df_exploded.schema["body"].dataType.fields]:
    # attach next row id to products
    w = Window.orderBy("id")
    df_sell = df_exploded.withColumn("discount_row_id", lead("id").over(w))

    # separate dataframes with products and discounts
    df_sell = df_sell.filter(col("body.sellLine").isNotNull())\
        .select(col("id"), col("discount_row_id"), col("body.sellLine.*"), col("filename_id"))

    df_discount = df_exploded.filter(col("body.discountLine").isNotNull())\
        .select(col("id"), col("body.discountLine.value").alias("discount_value"), col("body.discountLine.isDiscount"))

    df_discount = df_discount.withColumn("discount_value", col("discount_value") / 100)
    trigger = True
else:
    df_sell = df_exploded.filter(col("body.sellLine").isNotNull())\
        .select(col("id"), col("body.sellLine.*"), col("filename_id"))

df_sell = df_sell.withColumn("total", col("total") / 100)

#=== category and type identification ===
df_cat = category_id_func(df_sell)

#=== date ===
df_header = df.select(explode("header").alias("header"))
df_timestamp = df_header.select("header.headerData.date").where(col("header.headerData.date").isNotNull())
df_date = date_time_extration_func(df_timestamp)

#=== amount of peases transformation ===
df_amount = amount_to_countable_trans(df_sell)

#=== mass extraction ===
df_mass = mass_extraction_func(df_sell)


#=== final data frame ===
df_final = df_sell.join(df_amount, on="id", how="inner")\
        .join(df_mass, on="id", how="inner")\
        .join(df_cat, on="id", how="inner")\
        .join(df_date, on="filename_id", how="inner")\
        .withColumn("mass_kg", (col("mass_kg") * col("amount")))\
        .withColumn("Currency", lit("PLN"))\
        .withColumn("Ex rate (cur-cur)", lit(1))\
        .withColumn("Date", col("date"))\
        .withColumn("Time", col("time"))\
        .withColumn("Place", lit("Biedronka"))\

if trigger:
    df_final = df_final.join(df_discount, df_sell.discount_row_id == df_discount.id, how="left")\
        .withColumn("total", round(when(col("isDiscount").isNotNull(), (col("total") - col("discount_value"))).otherwise(col("total")), 2))\
        .withColumn("isDiscount", when(col("isDiscount").isNotNull(), "yes").otherwise("no"))\
        .withColumn("Amount in PLN", (col("total") * col("Ex rate (cur-cur)")))\
        .select(col("total").alias("Cost"),\
            col("amount").alias("Amount ps"),\
            col("mass_kg").alias("Amount kg (L)"),\
            col("isDiscount").alias("Discount"),\
            col("Currency"),\
            col("Ex rate (cur-cur)"),\
            col("Amount in PLN"),\
            col("category").alias("Category"),\
            col("type").alias("Type"),\
            col("Date"),\
            col("Time"),\
            col("Place"))
else:
    df_final = df_final.withColumn("Discount", lit("no"))\
        .withColumn("Amount in PLN", (col("total") * col("Ex rate (cur-cur)")))\
        .select(col("total").alias("Cost"),\
            col("amount").alias("Amount ps"),\
            col("mass_kg").alias("Amount kg (L)"),\
            col("Discount"),\
            col("Currency"),\
            col("Ex rate (cur-cur)"),\
            col("Amount in PLN"),\
            col("category").alias("Category"),\
            col("type").alias("Type"),\
            col("Date"),\
            col("Time"),\
            col("Place"))

# sort checks by date
df_final = df_final.orderBy(col("Date").asc())

# fill in null values
df_final = df_final.fillna(0)


output_path = "/opt/bitnami/spark/result_csv/test_run_1"
df_final.coalesce(1).write.mode("overwrite")\
    .option("header", True)\
    .csv(output_path)

spark.stop()

google_sheet_loader()