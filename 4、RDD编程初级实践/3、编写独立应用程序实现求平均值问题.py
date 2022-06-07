"""
Time:     2022/6/7 20:58
Author:   LEHOSO
Version:  V 0.1
File:     3、编写独立应用程序实现求平均值问题.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
Aglorithm = sc.textFile("file:///root/test/Aglorithm.txt")
Database = sc.textFile("file:///root/test/Database.txt")
Python = sc.textFile("file:///root/test/Python.txt")

lines = Aglorithm.union(Database).union(Python)
data = lines.map(lambda x: x.split(" ")) \
    .map(lambda x: (x[0], (int(x[1]), 1)))
res = data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
result = res.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))
result.repartition(1).saveAsTextFile("file:///root/test/Result")
R = sc.textFile("file:///root/test/Result")
R.foreach(print)
