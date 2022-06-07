## 一、HDFS的操作：
### 1、在虚拟机本地盘/usr/local下创建文件夹data，在此创建一个名为才d1.txt的文件，
第一行：“Spark And Hadoop”、
第二行：“Hadoop  And Apache”、
第三行：“Spark And Apache”;

```bash
 #！/bin/bash
mkdir /usr/local/data
vim /usr/local/data/d1.txt
```

### 2、在hdfs中新建目录myspark，并通过命令查看创建结果；

```bash
 #！/bin/bash
hdfs dfs -mkdir /myspark
hdfs dfs -ls /
```

### 3、将本地盘/usr/local/data下方的d1.txt上传到hdfs的myspark目录下方，并查看结果；

```bash
 #！/bin/bash
hdfs dfs -put /usr/local/data/d1.txt /myspark
hdfs dfs -cat /myspark/d1.txt
```
### 4、在hdfs中的目录spark下方再创建二级目录mydata，并将本地磁盘的d1.txt文件上传至此，通过命令查看结果； 
```bash
 #！/bin/bash
hdfs dfs -mkdir /myspark/mydata
hdfs dfs -put /usr/local/data/d1.txt /myspark/mydata
hdfs dfs -cat /myspark/mydata/d1.txt
hdfs dfs -ls /myspark
```
### 5、在hdfs上查看d1.txt文件的内容，通过命令查看结果；
```bash
 #！/bin/bash
hdfs dfs -cat /myspark/mydata/d1.txt
hdfs dfs -cat /myspark/d1.txt
```
### 6、将hdfs上mydata目录中的d1.txt文件删除，通过命令查看结果；
```bash
 #！/bin/bash
hdfs dfs -ls /myspark/mydata
hdfs dfs -rm -r -skipTrash /myspark/mydata/d1.txt
hdfs dfs -ls /myspark/mydata
```
### 7、将hdfs上myspark目录中的d1.txt下载到虚拟机本地盘/usr/local/下方，并查看结果；
```bash
 #！/bin/bash
hdfs dfs -get /myspark/d1.txt /usr/local/
ll /usr/local
```
### 8、删除hdfs上创建的目录myspark，通过命令查看结果。
```bash
 #！/bin/bash
hdfs dfs -ls /
hdfs dfs -rm -r /myspark
hdfs dfs -ls /
```