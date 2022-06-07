"""
Time:     2022/6/7 19:30
Author:   LEHOSO
Version:  V 0.1
File:     一、掌握RDD的3种创建方法.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *

sc = SparkContext()
# 1、将文件：student.txt上传到本地磁盘/usr/local/softwares中，
# 使用相应方法将其转换成RDD，并打印输出其结果；
# ls /usr/local/softwares |grep student.txt
student = sc.textFile("file:///usr/local/softwares/student.txt")
student.foreach(print)

# 2、将文件result_math.txt：上传到HDFS的myspark目录下，使用相应方法将其转换成RDD，并打印输出其结果；
# hdfs dfs -put  result_math.txt /myspark
# hdfs dfs -ls /myspark
result_math = sc.textFile("hdfs://master:9000//myspark/result_math.txt")
result_math.foreach(print)

# 3、创建一个1-100之间3的倍数的列表，使用相应方法将其转换成RDD，并打印输出其结果；
numbers = list(range(3, 100, 3))
print(numbers)
number = sc.parallelize(numbers)
number.foreach(print)

# 4、将上一题中result_math.txt，转换成RDD，
# 筛选出成绩等于100分的信息保存到新的RDD中，并打印显示出来；
result_math = sc.textFile("hdfs://master:9000//myspark/result_math.txt")
result_math.foreach(print)
lineWithMath = result_math.filter(lambda line: " 100" in line)
print("------------")
lineWithMath.foreach(print)

# 5、将实验1中文件d1.txt转换的RDD使用map转换，使用空格分离转换，并查看其结果。
d1 = sc.textFile("file:///usr/local/d1.txt")
word = d1.map(lambda line: line.split(" "))
word.foreach(print)

# 6、将文件d1.txt转换的RDD使用flatMap转换，使用空格分离转换，并查看其结果。
d1 = sc.textFile("file:///usr/local/d1.txt")
word = d1.flatMap(lambda line: line.split(" "))
