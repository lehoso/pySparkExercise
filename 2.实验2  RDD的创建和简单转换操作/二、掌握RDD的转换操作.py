"""
Time:     2022/6/7 19:48
Author:   LEHOSO
Version:  V 0.1
File:     二、掌握RDD的转换操作.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 7、创建一个列表
# arr=[("hadoop",2),("spark",1),("spark",2),
# ("python",1),("python",3),("java",2),("java",8)]
arr = [("hadoop", 2), ("spark", 1), ("spark", 2),
       ("python", 1), ("python", 3), ("java", 2), ("java", 8)
       ]

# 1、将列表转换成RDD；
wordsRDD = sc.parallelize(arr)
wordsRDD.foreach(print)

# 2、使用groupByKey()转换对RDD进行转换，并查看结果；
wordsGBK = wordsRDD.groupByKey()
wordsGBK.foreach(print)

# 3、使用reduceByKey()转换对RDD进行转换，并查看结果；
wordsRBK = wordsRDD.reduceByKey(lambda a, b: a + b)
wordsRBK.foreach(print)
