"""
Time:     2022/6/7 23:11
Author:   LEHOSO
Version:  V 0.1
File:     Rdd 应用题.py
Describe: Write during the junior at CQUCC, Github link: https://github.com/lehoso
"""
"""
1、 在/root/test 目录下有 2 个文本文件
file1.txt 和file2.txt，
每个文件中有很多行数据， 每行数据由 3 个字段的值构成，
不同字段之间用逗号隔开，如下图所示。
其中 3 个字段分别是：orderid，userid，payment，
现要求通过 rdd 的各种转换操作实现求出 payment 字段 Top 值的前 5 个
P86
"""

from pyspark import *

sc = SparkContext()
rdd_file1 = sc.textFile("file:///root/test/file1.txt")
rdd_file2 = sc.textFile("file:///root/test/file2.txt")
# 两个RDD数据连接
rdd_file = rdd_file1.union(rdd_file2)
# 把空行和字段数量不等于3的丢弃，只保留orderid，userid，payment
result1 = rdd_file.filter(lambda line: (len(line.strip()) > 0)
                                       and (len(line.split(",")) == 3))
# 只保留payment字段
result2 = result1.map(lambda x: x.split(",")[2])
# 把每个元素转为整型
result3 = result2.map(lambda x: (int(x), ""))
# 键值对保留一个分区可以对整个分区元素排序
result4 = result3.repartition(1)
# 调用sortByKey降序排序
result5 = result4.sortByKey(False)
# 得到降序
result6 = result5.map(lambda x: x[0])
# 取出top5
result7 = result6.take(5)
print(result7)

for a in result7:
    print(a)
