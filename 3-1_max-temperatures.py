from typing import Tuple
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Max-Temperature")
sc = SparkContext(conf=conf)


def parse_line(line: str) -> Tuple[str, str, float]:
    field = line.split(',')
    ob_site = field[0]
    temperature_type = field[2]
    temperature = float(field[3])
    return (ob_site, temperature_type, temperature)


lines = sc.textFile("./data/1800.csv")
parse_lines = lines.map(parse_line)
filtered_rdd = parse_lines.filter(lambda x: "TMAX" in x[1])
kv_rdd = filtered_rdd.map(lambda x: (x[0], x[2]))
reduced_rdd = kv_rdd.reduceByKey(lambda x, y: max(x, y))
max_temperatures = reduced_rdd.collect()

for k, v in max_temperatures:
    print(f'{k}\t{v:.2f}')
