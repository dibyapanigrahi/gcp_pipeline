#If the data is already in GCS, we now can load it to our master node. Go to the DataProc 
#master node shell and use the gsutil command like this:

gsutil cp gs://[BUCKET NAME]/from-git/chapter-5/dataset/simple_file.csv ./

#Creating a Hive table on top of HDFS
CREATE EXTERNAL TABLE simple_table(
    col_1 STRING,
    col_2 STRING,
    col_3 STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location 'data/simple_file'
TBLPROPERTIES ("skip.header.line.count"="1")
;

#Accessing an HDFS file from PySpark
simple_file = sc.textFile('hdfs://packt-dataproc-cluster-m/data/simple_file/simple_file.csv')
simple_file.collect()