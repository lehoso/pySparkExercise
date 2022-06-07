"""
Time:     2022/6/7 22:10
Author:   LEHOSO
Version:  V 0.1
File:     1、学生数据集.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

# 成绩表
sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
resultMath = spark.sparkContext.textFile("file:///root/test/result_math.txt") \
    .map(lambda line: line.split("\t")) \
    .map(lambda p: Row(id=int(p[0]), course=p[1], score=int(p[2])))
schemaResultMath = spark.createDataFrame(resultMath)
schemaResultMath.createOrReplaceTempView("resultMath")
# schemaResultMath.show()
# 学生表
student = spark.sparkContext.textFile("file:///root/test/student.txt") \
    .map(lambda line: line.split("\t")) \
    .map(lambda p: Row(id=int(p[0]), name=p[1]))
schemaStudent = spark.createDataFrame(student)
schemaStudent.createOrReplaceTempView("student")
# schemaStudent.show()

# 2）查询数学成绩是100分的同学有哪些；
spark.sql("select * from resultMath "
          "where score=100"
          ).show()

# 3）按照成绩高低排序学生和数学成绩信息
spark.sql(
    "SELECT name,score FROM resultMath AS math "
    "INNER JOIN student ON math.id = student.id "
    "ORDER BY math.score desc"
).show()
