"""
Time:     2022/6/7 21:05
Author:   LEHOSO
Version:  V 0.1
File:     1、使用rdd转换与文件存储.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

# 1.将本地路径/root/test下的文件chapter4-data01.txt
from pyspark import *

sc = SparkContext()
chapter4 = sc.textFile("file:///root/test/chapter4-data01.txt")

# （1）转换为rdd，并将结果命名为writeback保存到root/test下,显示输出；
chapter4.saveAsTextFile("file:///root/test/writeback")
writeBack = sc.textFile("file:///root/test/writeback")
writeBack.foreach(print)

# （2）计算每个人选修了几门课，并将结果命名为read保存到root/test下；
res = chapter4.map(lambda x: x.split(","))
student = res.map(lambda x: (x[0], 1))
each_student = student.reduceByKey(lambda x, y: x + y)
each_student.saveAsTextFile("file:///root/test/read")
read = sc.textFile("file:///root/test/read")
read.foreach(print)

# （3）计算每门课有几个人选修，并将分区设置为3后，命名为write保存到root/test下；
chapter4_3 = sc.textFile("file:///root/test/chapter4-data01.txt", 3)
res = chapter4_3.map(lambda x: x.split(","))
score = res.map(lambda x: (x[1], 1))
each_score = score.reduceByKey(lambda x, y: x + y)
each_score.foreach(print)
each_score.saveAsTextFile("file:///root/test/write")
scores = sc.textFile("file:///root/test/write")
scores.foreach(print)

# （4）显示选修Software课程的最高分和最低分；
res = chapter4.map(lambda x: x.split(","))
Software = res.filter(lambda x: x[1] == "Software")
max = Software.max(lambda x: int(x[2]))
min = Software.min(lambda x: x[2])
print(max)
print(min)

# （5）显示Jay的最高分和最低分；
res = chapter4.map(lambda x: x.split(","))
Software = res.filter(lambda x: x[0] == "Jay")
max = Software.max(lambda x: int(x[2]))
min = Software.min(lambda x: int(x[2]))
print(max)
print(min)
