"""
Time:     2022/6/7 20:04
Author:   LEHOSO
Version:  V 0.1
File:     2、RDD的分区.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 2、将word.txt文档转换成RDD，并设置为3个分区，命名为rdd3，并分别显示rdd3与分区个数。
rdd3 = sc.textFile("file:///usr/local/data/words.txt", 3)
rdd3.foreach(print)
print("显示rdd3这个RDD的分区数量：" + str(len(rdd3.glom().collect())))

# 3、重新设置分区个数为2，并显示。
rdd3 = rdd3.repartition(2)
print("显示重置之后的rdd3这个RDD的分区数量：" + str(len(rdd3.glom().collect())))

