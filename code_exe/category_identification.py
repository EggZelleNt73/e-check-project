from pyspark.sql.functions import *
import json

def category_id_func(df_sell):
    #reading json file with types and categories
    product_map = []

    with open("/opt/bitnami/spark/code_exe/category_type.json", "r") as f:
        product_map = json.load(f)
    
    # reading products dataframe
    df_cat = df_sell.select(col("id"), col("name"))\
        .withColumn("category", lit(None))\
        .withColumn("type", lit(None))
    
    expr_cat, expr_type = None, None

    # matching category and type
    for item in product_map:
        pattern = "|".join(item["keywords"])
        condition = col("name").rlike(pattern)

        # category expression
        expr_cat = when(condition, lit(item["category"])) if expr_cat is None else expr_cat.when(condition, lit(item["category"]))
        
        # type expression
        expr_type = when(condition, lit(item["type"])) if expr_type is None else expr_type.when(condition, lit(item["type"]))

    df_cat = df_cat.withColumn("category", expr_cat.otherwise("Other"))\
        .withColumn("type", expr_type.otherwise("Other"))\
        .select(col("id"), col("category"), col("type"))
    
    return df_cat
