from pyspark.sql.functions import *

def amount_to_countable_trans(df_prod):
    df_amount = df_prod.withColumn("amount_ps", when(col("quantity") < 1.0 , 1)\
        .otherwise(floor(col("quantity"))))\
        .select(
            col("id"),\
            col("amount_ps")
            )

    return df_amount
