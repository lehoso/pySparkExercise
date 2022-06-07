"""
Time:     2022/6/7 20:55
Author:   LEHOSO
Version:  V 0.1
File:     2、编写独立应用程序去重.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext("local", "test")
A = sc.textFile("file:///root/test/A.txt")
B = sc.textFile("file:///root/test/B.txt")
lines = A.union(B)
dis_lines = lines.distinct()
res = dis_lines.sortBy(lambda x: x)
res.repartition(1).saveAsTextFile("file:///root/test/C")
C = sc.textFile("file:///root/test/C")
C.foreach(print)
