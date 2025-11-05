from pyspark.sql.functions import col, date_format

def date_transformation_func(df):
    df_date = df.withColumn("date", date_format(col("date"), "MM/dd/yyyy"))\
        .select(col("id"), col("date"))
    
    return df_date

if __name__ == "__main__":
    date_transformation_func(None)