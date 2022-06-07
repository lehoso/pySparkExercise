"""
Time:     2022/6/7 22:16
Author:   LEHOSO
Version:  V 0.1
File:     2、编程和反射.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

""" 
1）使用编程方式将student.txt转换成DataFrame，
使用反射机制将result_math.txt转换成DataFrame
"""
# Student
sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
# 生成表头
schemaString = "name age"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split(" ")]
# 使用编程方式
schema = StructType(fields)
# 生成表记录
lines = sc.textFile("file:///root/test/student.txt")
parts = lines.map(lambda x: x.split("\t"))
student = parts.map(lambda p: Row(p[0].strip(), p[1]))
# 下面把“表头” 和“表中的记录”拼装在一起
schemaStudent = spark.createDataFrame(student, schema)
schemaStudent.createOrReplaceTempView("student")
schemaStudent.show()

# Result_math
# 生成表头
resultMath = spark.sparkContext \
    .textFile("file:///root/test/result_math.txt")\
    .map(lambda line: line.split("\t"))\
    .map(lambda p: Row(id=int(p[0]), course=p[1], score=int(p[2])))
schemaResultMath = spark.createDataFrame(resultMath)
schemaResultMath.createOrReplaceTempView("resultMath")
schemaResultMath.show()
