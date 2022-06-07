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
from pyspark.sql.types import *

# 2.将上题生成的rdd转换成DataFrame后并生成临时表进行查询完成下面题目：

sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
book = spark.sparkContext.textFile("file:///root/book.txt").map(lambda line: line.split(",")) \
    .map(lambda p: Row(bookName=p[0], link=p[1], country=p[2], author=p[3],
                       translator=p[4], press=p[5], publicationDate=int(p[6]),
                       price=p[7], star=float(p[8]), score=float(p[9]),
                       evaluation=int(p[10]), info=p[11]))
schemaBook = spark.createDataFrame(book)
schemaBook.createOrReplaceTempView("book")
# 1)查询“人民文学出版社”出版的书籍的书名，作者和价格；
spark.sql("select bookName ,author, price "
          "from book where press=' 人民文学出版社'"
          ).show()

# 2)查询评价人数超过100000的书籍的书名和评分；
spark.sql("seLect bookName, score "
          "from book where evaluation >= 100000"
          ).show()

# 3)查询不同星级书籍的数量。
spark.sql("SELECT star,COUNT(star) "
          "FROM book GROUP BY star"
          ).show()
