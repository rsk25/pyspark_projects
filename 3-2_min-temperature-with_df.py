from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName('Min-Temperature').getOrCreate()

# provide schema
schema = StructType(
    [
        StructField('station_id', StringType(), True),
        StructField('date', IntegerType(), True),
        StructField('measure_type', StringType(), True),
        StructField('temperature', FloatType(), True)
    ]
)

# read as dataframe
df = spark.read.schema(schema).csv('./data/1800.csv')
df.printSchema()

# filter only TMIN entries
min_temps = df.filter(df.measure_type == 'TMIN')

# group by station
station_temps = min_temps.select('station_id', 'temperature')
min_temps_by_station = station_temps.groupBy('station_id').min('temperature')
min_temps_by_station.show()

# convert to fahrenheit
min_temps_by_station_fahr = min_temps_by_station.withColumn('temperature',
                                                            func.round(func.col('min(temperature)') * 0.1 * (9.0 / 5.0) + 32.0, 2))
min_temps_by_station_fahr = min_temps_by_station_fahr.sort('temperature')

results = min_temps_by_station_fahr.collect()

for result in results:
    print(f'{result[0]}\t{result[1]:.2f}F')

spark.stop()
