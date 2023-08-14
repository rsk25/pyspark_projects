from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs


def load_movie_names():
    movie_names = {}
    with codecs.open('./data/ml-100k/u.item', 'r', encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movie_names[int(fields[0])] = fields[1]
    return movie_names


spark = SparkSession.builder.appName('Popular_movies').getOrCreate()
name_dict = spark.sparkContext.broadcast(load_movie_names())

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
movie_cnts = movies_df.groupBy('movie_id').count()

# UDF that looks up the name from the broadcasted dictionary


def lookup_name(movie_id):
    return name_dict.value[movie_id]


lookup_name_udf = func.udf(lookup_name)

# Add a movie title column
movies_with_names = movie_cnts.withColumn(
    'movie_title', lookup_name_udf(func.col('movie_id'))
)

# Sort the results
sorted_movies_with_names = movies_with_names.orderBy(func.desc('count'))

sorted_movies_with_names.show(10, False)

spark.stop()
