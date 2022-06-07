"""
Time:     2022/6/7 21:47
Author:   LEHOSO
Version:  V 0.1
File:     2、DataFrame创建与保存.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""

from pyspark import *
from pyspark.sql import *

spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()

# 2.将本地路径/root/test下的文件people.txt创建DataFrame后保存成newpeople.txt，
# 查看并读取显示。
people = spark.read.text("file:///root/test/people.txt")
people.write.text("file:///root/test/newpeople.txt")
newpeopLe = spark.read.text("file:///root/test/newpeople.txt")
newpeopLe.show()
