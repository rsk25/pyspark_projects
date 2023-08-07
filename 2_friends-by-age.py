from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2]) # `fields` initally has strings
    numFriends = int(fields[3])
    return (age, numFriends)

PATH = "./data/fakefriends.csv"
lines = sc.textFile(PATH)
rdd = lines.map(parseLine)
# The following line will make it possible to count the number of occurrences by adding a 1
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
