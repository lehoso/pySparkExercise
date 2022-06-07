"""
Time:     2022/6/7 19:55
Author:   LEHOSO
Version:  V 0.1
File:     1、spark之RDD的创建与转换操作.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 1、使用三种键值对RDD的创建方式创建rdd0，rdd1，rdd2。内容自已定义
## 第一种
lines = sc.textFile("file:///usr/local/d1.txt")
lines.foreach(print)
rdd0 = lines.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))
rdd0.foreach(print)

## 第二种
list = ["hadoop", "Spark", "Hive", "Spark"]
rdd = sc.parallelize(list)
rdd1 = rdd.map(lambda word: (word, 1))
rdd1.foreach(print)

## 第三种
rdd2 = sc.parallelize(((1, 2), (3, 4), (5, 6)))
rdd2.foreach(print)
