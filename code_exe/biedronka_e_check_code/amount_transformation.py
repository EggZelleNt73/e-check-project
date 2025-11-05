from pyspark.sql.functions import col, when, lit, floor

def amount_to_countable_trans(df_prod):
    df_amount = df_prod.withColumn("amount_ps", when(col("quantity") == floor(col("quantity")) , col("quantity"))\
        .otherwise(lit(1)))\
        .select(
            col("id"),\
            col("amount_ps")
            )

    return df_amount

if __name__ == "__main__":
    amount_to_countable_trans(None)