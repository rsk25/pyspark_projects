from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import IntegerType, FloatType, StructField, StructType

spark = SparkSession.builder.appName('Customer-sum').getOrCreate()

schema = StructType(
    [
        StructField('customer_id', IntegerType(), True),
        StructField('item_id', IntegerType(), True),
        StructField('amount_spent', FloatType(), True)
    ]
)

df = spark.read.schema(schema).csv('./data/customer-orders.csv')
df = df.select('customer_id', 'amount_spent')
df = df.groupBy('customer_id').sum('amount_spent')
df = df.withColumn('amount_spent', func.round(
    func.col('sum(amount_spent)'), 2)).select('customer_id', 'amount_spent').sort('amount_spent')

df.show(df.count())

spark.stop()
