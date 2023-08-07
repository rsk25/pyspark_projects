from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

df = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("./data/fakefriends-header.csv")

table = df.cache()
table.createOrReplaceTempView("Friends")

query = '''
    SELECT 
        age, 
        ROUND(AVG(friends),2) AS number_of_friends
    FROM
        Friends
    GROUP BY
        age
    ORDER BY
        age DESC
'''
friends_by_age = spark.sql(query)

friends_by_age.show()

spark.stop()
