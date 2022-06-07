"""
Time:     2022/6/7 20:33
Author:   LEHOSO
Version:  V 0.1
File:     1、pyspark交互式编程.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

from pyspark import *

sc = SparkContext()

rdd = sc.textFile("file:///root/test/chapter4-data01.txt")
chapter4 = rdd.map(lambda line: line.split(","))

# (1)该系多少名学生
student = chapter4.map(lambda x: (x[0], " "))
students = student.groupByKey()
print(students.count())

# (2)该系总共开设多少门课程
res = chapter4.map(lambda x: x[1])
dis_res = res.distinct().count()
print(dis_res)

# (3)Tom同学的总成绩平均分是多少
Tom = chapter4.filter(lambda x: x[0] == "Tom")
score = Tom.map(lambda x: int(x[2]))
num = score.count()
sum_score = score.reduce(lambda x, y: x + y)
avg = sum_score / num
print(avg)

# (4)每名同学的选修的课程门数
student = chapter4.map(lambda x: (x[0], 1))
each_student = student.reduceByKey(lambda x, y: x + y)
# print(each_student.foreach(print))
print(each_student.top(5))

# (5)该系DataBase课程共有多少人选修。
DataBase = chapter4.filter(lambda x: x[1] == "DataBase").count()
print(DataBase)

# (6)各门课程的平均分是多少
course = chapter4.map(lambda x: (x[1], (int(x[2]), 1)))
temp = course.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg = temp.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2)))
print(avg.foreach(print))

# (7)使用累加器计算共有多少人选修DataBase这门课
DataBase = chapter4.filter(lambda x: x[1] == "DataBase")
accum = sc.accumulator(0)
DataBase.foreach(lambda x: accum.add(1))
print(accum.value)
