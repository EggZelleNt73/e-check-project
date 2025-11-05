from pyspark.sql.functions import explode, col, monotonically_increasing_id, input_file_name, lit, when, round, regexp_replace
from category_identification import category_identification_func
from biedronka_e_check_code.prod_sell_separation import prod_sell_separation_func
from biedronka_e_check_code.date_extraction import date_time_extration_func
from biedronka_e_check_code.amount_transformation import amount_to_countable_trans
from biedronka_e_check_code.mass_extraction import mass_extraction_func

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
    
    # Transforming quantity column for later ease of use
    df_prod = df_prod.withColumn("quantity", regexp_replace(col("quantity"), ",", ".").try_cast("double"))

    # Amount of peases transformation
    df_amount = amount_to_countable_trans(df_prod)

    # Mass extraction
    df_mass = mass_extraction_func(df_prod)
    
    # Joining all data frames
    df_joined = None

    if not df_disc.rdd.isEmpty():
        df_joined = df_prod.join(df_disc, df_prod.discount_row_id == df_disc.id, how="left")
    
    df_joined = df_joined.join(df_cat, on="id", how="inner")\
        .join(df_date, on="filename_id", how="inner")\
        .join(df_mass, on="id", how="inner")\
        .join(df_amount, on="id", how="inner")
    
    # Final adjustments
    df_final = df_joined.withColumn("mass_kg", (col("mass_kg") * col("amount_ps")))\
        .withColumn("currency", lit("PLN"))\
        .withColumn("ex_rate", lit(1))\
        .withColumn("date", col("date"))\
        .withColumn("time", col("time"))\
        .withColumn("place", lit("Biedronka"))\
        .withColumn("total", round(when(col("isDiscount").isNotNull(), ((col("total") - col("discount_value")) / 100)).otherwise(col("total") /100 ), 2))\
        .withColumn("isDiscount", when(col("isDiscount").isNotNull(), "yes").otherwise("no"))\
        .withColumn("amount_pln", col("total"))\
        .select(
            col("total").alias("cost"),\
            col("amount_ps"),\
            col("mass_kg"),\
            col("isDiscount").alias("discount"),\
            col("currency"),\
            col("ex_rate"),\
            col("amount_pln"),\
            col("category"),\
            col("type"),\
            col("date"),\
            col("time"),\
            col("place")
        )
    
    df_final = df_final.fillna(0)

    return df_final

if __name__ == "__main__":
    run_biedronka_execution(None, None)

