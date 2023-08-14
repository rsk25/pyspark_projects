from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("Popular_movies").getOrCreate()

# Create schema
schema = StructType(
    [
        StructField('user_id', IntegerType(), True),
        StructField('movie_id', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ]
)

movies_df = spark.read.option('sep', '\t') \
    .schema(schema).csv('./data/ml-100k/u.data')
top_movies_ids = movies_df.groupBy('movie_id') \
    .count().orderBy(func.desc('count'))
top_movies_ids.show(10)

spark.stop()
