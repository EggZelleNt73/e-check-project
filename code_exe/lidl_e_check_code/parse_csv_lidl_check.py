import io, os, glob
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id, floor, lit, col, when, abs
from category_identification import category_identification_func
from lidl_e_check_code.date_transformation import date_transformation_func  

def run_lidl_execution(df_lidl, df_json):
    df_lidl = df_lidl.withColumn("id", monotonically_increasing_id())

    # Category and type mapping 
    df_cat = category_identification_func(df_lidl, df_json)

    # Date transformation
    df_date = date_transformation_func(df_lidl).alias("d")
    
    # Joining data frames
    df_lidl = df_lidl.join(df_cat, on="id", how="inner")\
        .join(df_date,  on="id", how="inner")\
        .withColumn("cost", (col("price") - abs(col("discount_value"))))\
        .withColumn("amount_ps", floor(col("quantity")).cast("int"))\
        .withColumn("mass_kg", when(col("quantity") == floor(col("quantity")), lit(0)).otherwise(col("quantity")))\
        .withColumn("currency", lit("PLN"))\
        .withColumn("ex_rate", lit(1))\
        .withColumn("amount_pln", col("Cost"))\
        .withColumn("place", lit("Lidl"))\
        .select(
            col("cost"),
            col("amount_ps"),
            col("mass_kg"),
            col("discount"),
            col("currency"),
            col("ex_rate"),
            col("amount_pln"),
            col("category"),
            col("type"),\
            col("d.date").alias("date"),\
            col("time"),\
            col("place")
        )
    
    # Final adjustments
    df_final = df_lidl.fillna(0)

    return df_final

