from pyspark import SparkConf, SparkContext
from typing import Tuple

DATA_PATH = './data'


def parse_lines(line: str) -> Tuple[int, float]:
    field = line.split(',')
    customer_id = int(field[0])
    spending = float(field[2])
    return (customer_id, spending)


conf = SparkConf().setMaster('local').setAppName('Customer Sum')
sc = SparkContext(conf=conf)
rdd = sc.textFile(DATA_PATH+'/customer-orders.csv')
parsed_rdd = rdd.map(parse_lines)
grouped_sum_rdd = parsed_rdd.reduceByKey(lambda x, y: x+y)
ordered_rdd = grouped_sum_rdd.sortBy(lambda x: x[1], ascending=False)
ordered_rdd = ordered_rdd.mapValues(lambda x: round(x, 2))
result_rdd = ordered_rdd.collect()

for result in result_rdd:
    print(f'id:{result[0]}, sum:{result[1]}')
