"""
Time:     2022/6/7 21:31
Author:   LEHOSO
Version:  V 0.1
File:     2、Spark SQL基本操作.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

"""
本地路径/root/test下有文件employee.json
为employee.json创建DataFrame，并写出Python语句完成下列操作：
"""
from pyspark import *
from pyspark.sql import *

spark = SparkSession.builder.config(conf=SparkConf(), ).getOrCreate()
employee = spark.read.json("file:///root/test/employee.json")

# （1）查询所有数据；
employee.show()

# （2）查询所有数据，并去除重复的数据；
employee.distinct().show()

# （3）查询所有数据，打印时去除id字段；
employee.drop('id').show()

# （4）筛选出age>30的记录；
employee.filter(employee["age"] > 30).show()

# （5）将数据按age分组；
employee.groupby("age").count().show()

# （6）将数据按name升序排列；
employee.sort(employee["name"].asc()).show()

# （7）取出前3行数据；
print(employee.head(3))

# （8）查询所有记录的name列，并为其取别名为username；
employee.select(employee["name"].alias("username")).show()

# （9）查询年龄age的平均值；
employee.agg({"age": "mean"}).show()

# （10）查询年龄age的最小值。
employee.agg({"age": "min"}).show()
