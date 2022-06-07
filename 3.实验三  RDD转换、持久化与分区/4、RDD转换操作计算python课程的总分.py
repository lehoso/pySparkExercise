"""
Time:     2022/6/7 20:23
Author:   LEHOSO
Version:  V 0.1
File:     4、RDD转换操作计算python课程的总分.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

"""
10、将python.txt文件转换为rdd10，并通过RDD转换操作计算python课程的总分（尝试有几种方法）。
文件内容如下
"""
# 181001 python 96
# 181002 python 64
# 181003 python 89
# 181004 python 75
# 181005 python 84
# 181006 python 80
# 181007 python 76
# 181008 python 83
# 181009 python 86
# 181010 python 70
# 181011 python 89
# 181012 python 91

from pyspark import *

sc = SparkContext()

rdd = sc.textFile("file:///usr/local/data/python.txt")
# rdd.foreach(print)

# 方法一
rdd1 = rdd.map(lambda line: line.split(" "))
# rdd1.foreach(print)
rdd2 = rdd1.map(lambda x: (x[1], int(x[2])))
# rdd2.foreach(print)
rdd3 = rdd2.reduceByKey(lambda a, b: a + b)
rdd3.foreach(print)

# 方法二
rdd4 = rdd2.groupByKey()
# rdd4.foreach(print)
rdd4.map(lambda x: (x[0], sum(x[1])))
# rdd4.foreach(print)
rdd4.mapValues(lambda x: sum(x)).foreach(print)
