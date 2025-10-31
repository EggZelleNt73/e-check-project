from pyspark.sql.functions import *

def mass_extraction_func(df_sell):
#=== mass extraction ===
    mass_pattern = r"(\d+(?:[.,]\d+)?)(g|kg|ml|l|L)"

    # extract numeric value and unit
    df_with_mass = (
        df_sell.withColumn("mass_value_raw", regexp_extract(col("name"), mass_pattern, 1))\
            .withColumn("mass_unit", regexp_extract(col("name"), mass_pattern, 2))
    )

    # convert string to float (replace comma with dot for decimals)
    df_with_mass = df_with_mass.withColumn("mass_value", regexp_replace(col("mass_value_raw"),",", ".").try_cast("double"))

    # normalize everything to kilograms
    df_mass = df_with_mass.withColumn("mass_kg", when((col("mass_unit") == "g") | (col("mass_unit") == "G") | (col("mass_unit") == "ml"), col("mass_value") / 1000)\
        .when(col("mass_unit").isin("kg", "KG", "L", "l"), col("mass_value"))\
        .otherwise(None))\
        .select(col("id"), col("mass_kg"))
    
    return df_mass
