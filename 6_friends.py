from pyspark.sql import SparkSession, Row

# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def map_func(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")),
               age=int(fields[2]), num_friends=int(fields[3]))


sc = spark.sparkContext.textFile("./data/fakefriends.csv")
people = sc.map(map_func)

# Infer schema and register the DataFrame as a table.
schema_people = spark.createDataFrame(people).cache()
schema_people.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table
query = '''
    SELECT * 
    FROM people 
    WHERE age >= 13 AND age <= 19
'''
teenagers = spark.sql(query)

for teen in teenagers.collect():
    print(teen)

# using functions instead of queries
schema_people.groupBy("age").count().orderBy("age").show()

spark.stop()
