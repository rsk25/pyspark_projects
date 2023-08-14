from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

spark = SparkSession.builder.appName('Popular-superhero').getOrCreate()

schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True)
    ]
)

names = spark.read.schema(schema).option('sep', ' ')\
    .csv('./data/Marvel_Names.txt')
lines = spark.read.text('./data/Marvel_Graph.txt')

tmp_df = lines.withColumn('id', func.split(func.col('value'), ' ')[0])
tmp_df = tmp_df.withColumn(
    'connections',
    func.size(func.split(func.col('value'), ' ')) - 1
)
connections = tmp_df.groupBy('id')\
    .agg(func.sum('connections').alias('connections'))

least_popular = connections.sort(func.col('connections').asc()).first()
least_popular_name = names.filter(
    func.col('id') == least_popular[0]).select('name').first()

print(f'{least_popular_name[0]}: {least_popular[1]} Connections')

spark.stop()
