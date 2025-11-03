from pyspark.sql.functions import col, lit, when, explode
import json

def category_identification_func(df, df_json):

    df_map = df_json.withColumn("keyword", explode(col("keywords"))).drop("keywords")

    df_items = df.select(col("id"), col("item_name"))

    df_joined = df_items.join(df_map, df_items["item_name"].contains(df_map["keyword"]), "left")

    df_result = df_joined.fillna({"category":"Other", "type":"Other"}).select(col("id"), col("category"), col("type"))

    return df_result

