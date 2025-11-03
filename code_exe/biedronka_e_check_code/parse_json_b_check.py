from pyspark.sql.functions import explode, col, monotonically_increasing_id, input_file_name
from category_identification import category_identification_func
from biedronka_e_check_code.prod_sell_separation import prod_sell_separation_func
from biedronka_e_check_code.date_extraction import date_time_extration_func
from biedronka_e_check_code.amount_transformation import amount_to_countable_trans

def run_biedronka_execution(df_bied, df_json):
    # Explode data from json
    df_exploded = df_bied.select(explode("body").alias("body"))

    # Add id to each row and filename it comes from
    df_exploded = df_exploded.withColumn("id", monotonically_increasing_id())\
        .withColumn("filename_id", input_file_name())
    
    # Separation of discount and products
    df_prod, df_disc = prod_sell_separation_func(df_exploded)

    # Category and type identification
    df_cat = category_identification_func(df_prod.withColumnRenamed("name", "item_name"), df_json)

    # Date extraction
    df_header = df_bied.select(explode("header").alias("header"))
    df_timestamp = df_header.select("header.headerData.date").where(col("header.headerData.date").isNotNull())
    df_date = date_time_extration_func(df_timestamp)

    # Amount of peases transformation
    df_amount = amount_to_countable_trans(df_prod)

    df_joined = df_prod.join(df_disc, df_prod.discount_row_id == df_disc.id, how="left")\
        .join(df_cat, on="id", how="inner")\
        .join(df_date, on="filename_id", how="inner")
    
    df_final = df_joined.fillna(0)

    return df_final

