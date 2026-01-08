# Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import expr

spark = (
    SparkSession
    .builder
    .appName("User Defined Functions")
    .master("spark://17e348267994:7077")
    .config("spark.executor.cores", 2)
    .config("spark.cores.max", 6)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)
# Read employee data

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

emp = spark.read.format("csv").option("header", True).schema(emp_schema).load("/data/output/3/emp.csv")

emp.rdd.getNumPartitions()

def bonus(salary):
    return int(salary) * 0.1

# Register as UDF
bonus_udf = udf(bonus)
spark.udf.register("bonus_sql_udf", bonus, "double")

# Create new column as bonus using UDF
emp.withColumn("bonus", expr("bonus_sql_udf(salary)")).show()

# Create new column as bonus without UDF

emp.withColumn("bonus", expr("salary * 0.1")).show()