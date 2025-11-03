from pyspark.sql.functions import col, lead
from pyspark.sql import Window

def prod_sell_separation_func(df_exploded):
    # Check if discount lines are present in the check
    body_fields = [f.name for f in df_exploded.schema["body"].dataType.fields]
    if "discountLine" in body_fields:
        # attach next row id to products
        w = Window.orderBy("id")
        df_prod = df_exploded.withColumn("discount_row_id", lead("id").over(w))

        # separate dataframes with products and discounts
        df_prod = df_prod.filter(col("body.sellLine").isNotNull())\
            .select(
                col("id"),\
                col("discount_row_id"),\
                col("body.sellLine.*"),\
                col("filename_id")
                )

        df_disc = df_exploded.filter(col("body.discountLine").isNotNull())\
            .select(
                col("id"),\
                col("body.discountLine.value").alias("discount_value"),\
                col("body.discountLine.isDiscount")
                )
        
        return df_prod, df_disc
    else:
        df_prod = df_exploded.filter(col("body.sellLine").isNotNull())\
            .select(
                col("id"),\
                col("body.sellLine.*"),\
                col("filename_id")
                )
    
    return df_prod
    