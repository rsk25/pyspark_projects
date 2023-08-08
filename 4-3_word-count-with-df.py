from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line into a dataframe
df = spark.read.text('./data/Book')

# split using a regular expression that extracts words
# default name of column is 'value'
words = df.select(func.explode(func.split(df.value, '\\W+')).alias('word'))
words.filter(words.word != "")

# normalize everything to lowercase
# by aliasing an existing column, you can replace the column with new values
lowercase_words = words.select(func.lower(words.word).alias('word'))

# count each word
word_cnt = lowercase_words.groupBy('word').count()

# Sort by counts
word_cnt_ord = word_cnt.sort('count')

word_cnt_ord.show(word_cnt_ord.count())
