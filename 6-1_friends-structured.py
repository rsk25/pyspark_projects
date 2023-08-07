from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

# If the data is structured, it is possible to create a DataFrame directly
df = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv('./data/fakefriends-header.csv')

print("Inferred Schema: ")
df.printSchema()

print("'name' column:")
df.select("name").show()

print("Filter out people over age 21:")
df.filter(df.age < 21).show()

print("Group by age:")
df.groupBy("age").count().show()

print("Add 10 years to everyone's age:")
df.select(df.name, df.age + 10).show()

spark.stop()
