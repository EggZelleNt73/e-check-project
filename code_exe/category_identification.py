from pyspark.sql.functions import col, lit, when, explode, lower
import json

def category_identification_func(df, df_json):

    df_map = df_json.withColumn("keyword", explode(col("keywords"))).drop("keywords")

    df_items = df.select(col("id"), col("item_name"))

    df_joined = df_items.join(df_map, lower(df_items["item_name"]).contains(lower(df_map["keyword"])), "left")

    df_result = df_joined.fillna({"category":"Other", "type":"Other"}).select(col("id"), col("category"), col("type"))

    return df_result

if __name__ == "__main__":
    category_identification_func(None, None)