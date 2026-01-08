# Create Spark Session

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Merge Data Frames") \
    .master("local[*]") \
    .getOrCreate()

# Example DataFrame 1
_data = [
    ["C101", "Akshay", 21, "22-10-2001"],
    ["C102", "Sivay", 20, "07-09-2000"],
    ["C103", "Aslam", 23, "04-05-1998"],
]

_cols = ["ID", "NAME", "AGE", "DOB"]

df_1 = spark.createDataFrame(data = _data, schema = _cols)
df_1.printSchema()
df_1.show(10, False)

# Example DataFrame 2
_data = [
    ["C106", "Suku", "Indore", ["Maths", "English"]],
    ["C110", "Jack", "Mumbai", ["Maths", "English", "Science"]],
    ["C113", "Gopi", "Rajkot", ["Social Science"]],
]

_cols = ["ID", "NAME", "ADDRESS", "SUBJECTS"]

df_2 = spark.createDataFrame(data = _data, schema = _cols)
df_2.printSchema()
df_2.show(10, False)

# Now before we can merge the dataframes we have to add the extra columns from either dataframes
from pyspark.sql.functions import lit

# Lets add missing columns from df_2 to df_1
for col in df_2.columns:
    if col not in df_1.columns:
        df_1 = df_1.withColumn(col, lit(None))
        
# Lets add missing columns from df_1 to df_2
for col in df_1.columns:
    if col not in df_2.columns:
        df_2 = df_2.withColumn(col, lit(None))
        
# View the dataframes
df_1.show()
df_2.show()

# Lets use unionByName to do the merge successfully
df = df_1.unionByName(df_2)
df.printSchema()
df.show(10, False)