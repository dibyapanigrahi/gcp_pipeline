# Generate Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Reading from Sockets")
    .master("local[*]")
    .getOrCreate()
)

df_raw = spark.readStream.format("socket").option("host","localhost").option("port", "9999").load()
df_raw.printSchema()

# Split the line into words
from pyspark.sql.functions import split

df_words = df_raw.withColumn("words", split("value", " "))

# Explode the list of words
from pyspark.sql.functions import explode

df_explode = df_words.withColumn("word", explode("words")).drop("value", "words")

# Aggregate the words to generate count
from pyspark.sql.functions import count, lit

df_agg = df_explode.groupBy("word").agg(count(lit(1)).alias("cnt"))

# Write the output to console streaming

df_agg.writeStream.format("console").outputMode("complete").start().awaitTermination()