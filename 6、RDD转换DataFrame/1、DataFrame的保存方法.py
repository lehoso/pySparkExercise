"""
Time:     2022/6/7 21:38
Author:   LEHOSO
Version:  V 0.1
File:     1、DataFrame的保存方法.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

# 1.将本地路径/root/test下的文件employee.json创建DataFrame后完成下面题目：
from pyspark import *
from pyspark.sql import *

sc = SparkContext()

spark = SparkSession.builder.config(conf=SparkConf(), ).getOrCreate()
employeeDF = spark.read.json("file:///root/test/employee.json")
# 1）将DataFrame保存为“employee.json”，查看并读取内容显示；
# employeeDF.write.json("file:///root/test/employeeT.json")
# employeeT = spark.read.json("file:///root/test/employeeT.json")
# employeeT.show()

# 2）选择“id”和“name”字段保存为“employee1.json”, 查看并读取内容显示；
# employee1 = employeeDF.select(employeeDF['id'], employeeDF['name'])
# employee1.write.json("file:///root/test/employee1.json")
# employee1 = spark.read.json("file:///root/test/employee1.json")
# employee1.show()

# 3）选择“name”字段保存为“employee2.txt”, 查看并读取内容显示。
# employee2 = employeeDF.select(employeeDF['name'])
# employee2.write.text("file:///root/test/employee2.txt")
# employee2 = spark.read.text("file:///root/test/employee2.txt")
# employee2.show()
