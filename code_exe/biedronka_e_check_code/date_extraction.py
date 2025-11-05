from pyspark.sql.functions import *

def date_time_extration_func(df_timestamp, timestamp_col="date"):
    df_ts = df_timestamp.withColumn("ts", to_timestamp(col(timestamp_col), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    df_date_time = df_ts.withColumn("date", date_format(col("ts"), "MM/dd/yyyy"))\
        .withColumn("time", date_format(col("ts"), "HH:mm"))\
        .withColumn("filename_id", input_file_name())\
        .select(
            col("filename_id"),\
            col("date"),\
            col("time")
            )
    
    return df_date_time

if __name__ == "__main__":
    date_time_extration_func(None, None)