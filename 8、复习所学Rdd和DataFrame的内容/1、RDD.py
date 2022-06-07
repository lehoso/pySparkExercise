"""
Time:     2022/6/7 22:31
Author:   LEHOSO
Version:  V 0.1
File:     2、DataFrame.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
# 1.现有book.txt，包含内容有
# （书名,链接,国家,作者,翻译者,出版社,出版时间,价格,星级,评分,评价人数,简介），
# 请将book.txt转换成rdd，使用rdd各种转换操作完成下面题目：
from pyspark import *
from pyspark.sql import *

sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()) \
    .getOrCreate()
rdd1 = sc.textFile("file:///root/book.txt").map(lambda x: x.split(","))
# 1）筛选出中国的书籍；
rdd2 = rdd1.map(lambda x: (x[0], x[2]))
country = rdd2.filter(lambda x: x[1] == '中')
country.foreach(print)

# 2）筛选出2010年之后出版的书籍；
rdd2 = rdd1.map(lambda x: (x[0], x[6]))
publicationDate = rdd2.filter(lambda x: int(x[1]) > 2010)
publicationDate.foreach(print)

# 3）按照评分高低将书籍排序；
rdd2 = rdd1.map(lambda x: (x[0], float(x[9])))
score = rdd2.sortBy(lambda x: x[1], False)
score.foreach(print)

# 4）按照出版时间从低到高排序书籍；
rdd2 = rdd1.map(lambda x: (x[0], int(x[6])))
publicationDateLH = rdd2.sortBy(lambda x: x[1])
publicationDateLH.foreach(print)

# 5）数出不同国家书籍的数量；
rdd2 = rdd1.map(lambda x: (x[2], 1))
country = rdd2.reduceByKey(lambda x, y: x + y)
country.foreach(print)
