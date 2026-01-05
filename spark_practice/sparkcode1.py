transaction_file = "test.parquet"
df_tran = spark.read.parquet(transaction_file)

df_tran.rdd.getNumPartitions()
df_tran.count()
df_tran.show(5,False)
