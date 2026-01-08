# Create the Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession 
    .builder 
    .appName("Streaming Process Files") 
    .config("spark.streaming.stopGracefullyOnShutdown", True) 
    .master("local[*]") 
    .getOrCreate()
)
# To allow automatic schemaInference while reading
spark.conf.set("spark.sql.streaming.schemaInference", True)

# Create the streaming_df to read from input directory
streaming_df = (
    spark
    .readStream
    .option("cleanSource", "archive")
    .option("sourceArchiveDir", "archive_dir")
    .option("maxFilesPerTrigger", 1)
    .format("json")
    .load("data/input/device_files/")
)
# To the schema of the data, place a sample json file and change readStream to read 
streaming_df.printSchema()
# streaming_df.show(truncate=False)

# Lets explode the data as devices contains list/array of device reading
from pyspark.sql.functions import explode

exploded_df = streaming_df.withColumn("data_devices", explode("data.devices"))

# Check the schema of the exploded_df, place a sample json file and change readStream to read 
exploded_df.printSchema()
#exploded_df.show(truncate=False)

# Flatten the exploded df
from pyspark.sql.functions import col

flattened_df = (
    exploded_df
    .drop("data")
    .withColumn("deviceId", col("data_devices.deviceId"))
    .withColumn("measure", col("data_devices.measure"))
    .withColumn("status", col("data_devices.status"))
    .withColumn("temperature", col("data_devices.temperature"))
    .drop("data_devices")
)

# Check the schema of the flattened_df, place a sample json file and change readStream to read 
flattened_df.printSchema()
#flattened_df.show(truncate=False)

# Write the output to console sink to check the output

(flattened_df
 .writeStream
 .format("csv")
 .outputMode("append")
 .option("path", "data/output/device_data.csv")
 .option("checkpointLocation", "checkpoint_dir")
 .start()
 .awaitTermination())