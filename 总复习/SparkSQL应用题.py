"""
Time:     2022/6/8 00:10
Author:   LEHOSO
Version:  V 0.1
File:     SparkSQL应用题.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

# /root/test下现有学生信息文档student.txt，
# 课程信息文档course.txt和成绩信息文档grade.txt
sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# student表
student = spark.sparkContext.textFile("file:///root/test/data/student.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(s_id=int(p[0]), s_name=p[1], sex=p[2], age=int(p[3])))
schemaStudent = spark.createDataFrame(student)
schemaStudent.createOrReplaceTempView("student")

# course表
course = spark.sparkContext.textFile("file:///root/test/data/course.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(c_id=int(p[0]), c_name=p[1], selected=int(p[2]), credit=int(p[3])))
schemaCourse = spark.createDataFrame(course)
schemaCourse.createOrReplaceTempView("course")

# course表
grade = spark.sparkContext.textFile("file:///root/test/data/grade.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(s_id=int(p[0]), c_id=int(p[1]), score=int(p[2])))
schemaGrade = spark.createDataFrame(grade)
schemaGrade.createOrReplaceTempView("grade")

# 1、查询181005学号学生的姓名和年龄；
spark.sql("select s_name,age from student where s_id == 181005 ").show()

# 2、查询孙慧选修课程的课号和成绩；（嵌套查询）
spark.sql("select c_id ,score "
          "from grade "
          "where s_id == "
          "(select s_id from student where s_name=='孙慧') "
          ).show()

# 3、查询选修大学物理课程的学分、课号和成绩；（连接查询）
spark.sql("SELECT  c.credit,c.c_id,g.score "
          "FROM course c ,grade g "
          "WHERE c.c_id=g.c_id AND c.c_name='大学物理'"
          ).show()
# 4、查询选修离散数学课程的学号、姓名、课号。（内连接查询）
spark.sql(
    "SELECT s.s_id,s_name,c.c_id FROM course c "
    "INNER JOIN grade g ON c.c_id=g.c_id "
    "INNER JOIN student s ON g.s_id = s.s_id "
    "WHERE c.c_name='离散数学'").show()
