"""
Time:     2022/6/7 21:49
Author:   LEHOSO
Version:  V 0.1
File:     3、反射机制方式创建成DataFrame.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
# 3.将本地路径/root/test下的文件chapter4-data01.txt
# 使用反射机制方式创建成DataFrame，显示输出后完成如下题目；
from pyspark import *
from pyspark.sql import *

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

chapter4 = spark.sparkContext \
    .textFile("file:///root/test/chapter4-data01.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(name=p[0], course=p[1], score=int(p[2])))
schemaChapter4 = spark.createDataFrame(chapter4)
schemaChapter4.createOrReplaceTempView("chapter4")
schemaChapter4.show()

# 1）查询选修DataBase课程的学生姓名和成绩；
dataBaseDF = spark \
    .sql("select name, score from chapter4 where course='DataBase'")
dataBaseDF.show()

# 2）查询Tom的总分和平均分；
Tom = spark.sql("select sum(score), avg(score) "
                "from chapter4 "
                "where name = 'Tom' ")
Tom.show()

# 3）查询每名同学选修课程的门数和总分，平均分；
student = spark.sql("select name , count (course), sum( score), avg(score) "
                    "from chapter4 "
                    "group by name")
student.show()

# 4）查询总共开设课程门数；
countCourse = spark.sql("select count (distinct(course)) "
                        "from chapter4")
countCourse.show()

# 5）查询每门课有多少人选修；
countCourse = spark.sql("select course, count(course) "
                        "from chapter4 "
                        "group by course")
countCourse.show()

# 6）计算每门课程的平均分。
avgCourse = spark.sql("select course , avg(score) "
                      "from chapter4 "
                      "group by course")
avgCourse.show()
