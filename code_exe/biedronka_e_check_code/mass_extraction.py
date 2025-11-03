from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, lower, lit, floor

def mass_extraction_func(df_prod):
    #=== mass extraction ===
    mass_pattern = r"(\d+(?:[.,]\d+)?)(g|kg|ml|l)"

    # extract numeric value and unit
    df_with_mass = (
        df_prod.withColumn("mass_value_raw", regexp_extract(col("name"), mass_pattern, 1))\
            .withColumn("mass_unit", lower(regexp_extract(col("name"), mass_pattern, 2)))
    )

    # convert string to float (replace comma with dot for decimals)
    df_with_mass = df_with_mass.withColumn("mass_value", regexp_replace(col("mass_value_raw"),",", ".").try_cast("double"))

    # normalize everything to kilograms
    df_mass = df_with_mass.withColumn("mass_kg",
        when(col("mass_unit").isin("g", "ml"), col("mass_value") / 1000)\
        .when(col("mass_unit").isin("kg", "l"), col("mass_value"))\
        .otherwise(None))
    
    df_mass = df_mass.withColumn("mass_kg", when(col("mass_kg").isNull(),
        when(col("quantity") != floor(col("quantity")), col("quantity")).otherwise(lit(0)))\
        .otherwise(col("mass_kg")))\
        .select(
            col("id"),\
            col("mass_kg")\
            )
    
    return df_mass
