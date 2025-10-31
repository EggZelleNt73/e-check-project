from pyspark.sql.functions import *

def amount_to_countable_trans(df_sell):
    df_amount = df_sell.withColumn("amount", when(col("quantity").contains(","), 1)\
        .otherwise(col("quantity").cast("int")))\
        .select(col("id"), col("amount"))

    return df_amount
