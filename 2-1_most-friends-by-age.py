from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("most_friends_by_age")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    name = fields[1]
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


PATH = "./data/fakefriends.csv"
lines = sc.textFile(PATH)
rdd = lines.map(parse_line)

max_by_age = rdd.groupByKey().mapValues(max)
results = sorted(max_by_age.collect())
for result in results:
    print(result)
