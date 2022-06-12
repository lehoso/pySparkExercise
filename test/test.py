"""
Time:     2022/6/7 19:11
Author:   LEHOSO
Version:  V 0.1
File:     test.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 1、求图书的平均销量
# 现有一组键值对（“spark”，2），（“hadoop”，6），（“hadoop”，4），（“spark”，6），
# 键值对的key表示图书名称，value表示某天的图书销量，现在需要计算每个键对应的平均值，
# 也就是计算每种图书的平均销量
rdd = sc.parallelize([("spark", 2), ("hadoop", 6), ("hadoop", 4), ("spark", 6)])
avg = rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .mapValues(lambda x: x[0] / x[1]).collect()
print(avg)
