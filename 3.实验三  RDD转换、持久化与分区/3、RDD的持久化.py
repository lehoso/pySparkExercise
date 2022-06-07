"""
Time:     2022/6/7 20:04
Author:   LEHOSO
Version:  V 0.1
File:     2、RDD的分区.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 4、将rdd3标记为持久化；
rdd3 = sc.textFile("file:///usr/local/data/words.txt", 3)
# rdd3.foreach(print)
# print("----------")
# rdd3.cache()
# print(rdd3.count())
# print(','.join(rdd3.collect()))

# 5、在rdd3中筛选出包含“good”的字符串保存到新的rdd4中，并打印显示；
rdd4 = rdd3.filter(lambda line: "good" in line)
# rdd4.foreach(print)

# 6、将rdd3使用flatMap转换，使用空格分离转换，保存成rdd5并查看其结果；
rdd5 = rdd3.flatMap(lambda line: line.split(" "))
# rdd5.foreach(print)

# 7、将rdd5使用map转换生成键值对，保存成rdd6，再使用groupByKey转换保存为rdd7，并查看其结果；
rdd6 = rdd5.map(lambda word: (word, 1))
# rdd6.foreach(print)
print("-------------")
rdd7 = rdd6.groupByKey()
# rdd7.foreach(print)

# 8、将rdd6，使用reduceByKey转换，计算词频，保存成rdd8并查看其结果；
rdd8 = rdd6.reduceByKey(lambda a, b: a + b)
# rdd8.foreach(print)

# 9、将rdd7计算词频，保存成rdd9并查看其结果；
rdd9 = rdd7.map(lambda t: (t[0], sum(t[1])))
rdd9.foreach(print)
