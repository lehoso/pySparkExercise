"""
Time:     2022/5/27 14:37
Author:   LEHOSO
Version:  V 0.1
File:     2、货品交易数据集.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *

"""
2.虚拟机root/test目录下有如下数据集：tbDate.txt，tbStock.txt，tbStockDetail.txt。
它们为货品交易数据集，其中每个订单可能包含多个货品，每个订单可以产生多次交易，不同的货品有不同的单价。
"""

sc = SparkContext()
spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
# tbDate.txt    dateid| years|theyear|month|day|weekday|week|quarter|period|halfmonth
tbDate = spark.sparkContext.textFile("file:///root/test/tbDate.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(dateid=p[0], years=int(p[1]), theyear=int(p[2]), month=int(p[3]),
                       day=int(p[4]), weekday=int(p[5]), week=int(p[6]), quarter=int(p[7]),
                       period=int(p[8]), halfmonth=int(p[9])))
schemaTbDate = spark.createDataFrame(tbDate)
schemaTbDate.createOrReplaceTempView("tbDate")

# tbStock.txt    ordernumber|locationid|dateid|
tbStock = spark.sparkContext.textFile("file:///root/test/tbStock.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(ordernumber=p[0], locationid=p[1], dateid=p[2]))
schemaTbStock = spark.createDataFrame(tbStock)
schemaTbStock.createOrReplaceTempView("tbStock")

# tbStockDetail.txt ordernumber|rownum|itemid|number|price|amount
tbStockDetail = spark.sparkContext.textFile("file:///root/test/tbStockDetail.txt") \
    .map(lambda line: line.split(",")) \
    .map(lambda p: Row(ordernumber=p[0], rownum=int(p[1]), itemid=p[2],
                       number=int(p[3]), price=float(p[4]), amount=float(p[5])
                       ))
schemaTbStockDetail = spark.createDataFrame(tbStockDetail)
schemaTbStockDetail.createOrReplaceTempView("tbStockDetail")

"""
3）计算所有订单中每年最畅销货品
第三步、用最大销售额和统计好的每个货品的销售额 join，以及用年 join，得到最畅销货品那一行信息
"""
spark.sql(
    "SELECT DISTINCT e.theyear, e.itemid, f.maxofamount "
    "FROM "
    "(SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a "
    "JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid "
    "GROUP BY c.theyear, b.itemid ) e "

    "JOIN "

    "(SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) "
    "AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber "
    "JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d "
    "GROUP BY d.theyear ) f "

    "ON e.theyear = f.theyear "
    "AND e.sumofamount = f.maxofamount "
    "ORDER BY e.theyear"
).show()

"""
3) 第二步、在第一步的基础上，统计每年单个货品中的最大金额
"""
# spark.sql(
#     "SELECT d.theyear, MAX(d.SumOfAmount) "
#     "AS MaxOfAmount "
#     "FROM "
#     "(SELECT c.theyear, b.itemid, SUM(b.amount) "
#     "AS SumOfAmount "
#     "FROM tbStock a "
#     "JOIN tbStockDetail b ON a.ordernumber = b.ordernumber "
#     "JOIN tbDate c ON a.dateid = c.dateid "
#     "GROUP BY c.theyear, b.itemid ) d "
#     "GROUP BY d.theyear"
# ).show()
"""
3) 第一步、求出每年每个货品的销售额
"""
# spark.sql(
#     "SELECT c.theyear, b.itemid, SUM(b.amount) "
#     "AS SumOfAmount "
#     "FROM tbStock a "
#     "JOIN tbStockDetail b ON a.ordernumber = b.ordernumber "
#     "JOIN tbDate c ON a.dateid = c.dateid "
#     "GROUP BY c.theyear, b.itemid"
# ).show()

"""
2) 计算所有订单每年最大金额订单的销售额
2) 第二步、以上一步查询结果为基础表，和表 tbDate 使用 dateid join，求出每年最大金额订单的销售额
"""
# spark.sql(
#     "SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount "
#     "FROM "
#     "(SELECT a.dateid, a.ordernumber, SUM(b.amount) "
#     "AS SumOfAmount "
#     "FROM tbStock a "
#     "JOIN tbStockDetail b ON a.ordernumber = b.ordernumber "
#     "GROUP BY a.dateid, a.ordernumber ) c "
#     "JOIN tbDate d ON c.dateid = d.dateid "
#     "GROUP BY theyear ORDER BY theyear DESC"
# ).show()
"""
2) 第一步、统计每年，每个订单一共有多少销售额
"""
# spark.sql("SELECT a.dateid, a.ordernumber, "
#           "SUM(b.amount) AS SumOfAmount "
#           "FROM tbStock a "
#           "JOIN tbStockDetail b "
#           "ON a.ordernumber = b.ordernumber "
#           "GROUP BY a.dateid, a.ordernumber")\
#     .show()
"""
1)计算所有订单中每年的销售单数、销售总额
三个表连接后以 count(distinct a.ordernumber) 计销售单数，以 sum(b.amount) 计销售总额
"""
# spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) "
#           "FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber "
#           "JOIN tbDate c ON a.dateid = c.dateid "
#           "GROUP BY c.theyear "
#           "ORDER BY c.theyear").show()
"""
链接来源
"""
# https://www.cnblogs.com/huanghanyu/p/12988891.html#_label6_0
