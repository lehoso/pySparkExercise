"""
Time:     2022/6/7 18:56
Author:   LEHOSO
Version:  V 0.1
File:     二、spark之RDD的创建.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 1、将本地文件d1.txt转换成RDD，并打印输出其结果；
lines = sc.textFile("file:///usr/local/d1.txt")
lines.foreach(print)

# 2、将hdfs上文件d1.txt转换成RDD，并打印输出其结果；
line = sc.textFile("hdfs://master:9000/d1.txt")
line.foreach(print)

# 3、使用列表形式创建RDD（列表内容为20以内的偶数）
data = [2, 4, 6, 8, 10, 12, 14, 16, 18]
rdd = sc.parallelize(data)
rdd.foreach(print)
