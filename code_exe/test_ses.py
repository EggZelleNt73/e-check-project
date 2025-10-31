from pyspark.sql import SparkSession


# Create Spark session
spark = SparkSession.builder \
    .appName("SimpleTransformationExample") \
    .getOrCreate()

# Example data: list of numbers
data = [1, 2, 3, 4, 5, 6]

# Create an RDD
rdd = spark.sparkContext.parallelize(data)

# Transformation: square each number
squared_rdd = rdd.map(lambda x: x ** 2)

# Transformation: filter numbers greater than 10
filtered_rdd = squared_rdd.filter(lambda x: x > 10)

# Action: collect results
result = filtered_rdd.collect()

print("Original numbers:", data)
print("Squared numbers:", squared_rdd.collect())
print("Filtered numbers (>10):", result)

# Stop Spark session
spark.stop()

