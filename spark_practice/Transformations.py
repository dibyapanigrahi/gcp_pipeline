# Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when
from pyspark.sql.functions import lit
from pyspark.sql.functions import to_date
from pyspark.sql.functions import current_date, current_timestamp
spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

# Emp Data & Schema

emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"],
    ["006","103","Jill Wong","32","Female","52000","2018-07-01"],
    ["007","101","James Johnson","42","Male","70000","2012-03-15"],
    ["008","102","Kate Kim","29","Female","51000","2019-10-01"],
    ["009","103","Tom Tan","33","Male","58000","2016-06-01"],
    ["010","104","Lisa Lee","27","Female","47000","2018-08-01"],
    ["011","104","David Park","38","Male","65000","2015-11-01"],
    ["012","105","Susan Chen","31","Female","54000","2017-02-15"],
    ["013","106","Brian Kim","45","Male","75000","2011-07-01"],
    ["014","107","Emily Lee","26","Female","46000","2019-01-01"],
    ["015","106","Michael Lee","37","Male","63000","2014-09-30"],
    ["016","107","Kelly Zhang","30","Female","49000","2018-04-01"],
    ["017","105","George Wang","34","Male","57000","2016-03-15"],
    ["018","104","Nancy Liu","29","Female","50000","2017-06-01"],
    ["019","103","Steven Chen","36","Male","62000","2015-08-01"],
    ["020","102","Grace Kim","32","Female","53000","2018-11-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"
emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
print(emp.rdd.getNumPartitions())
print(emp.rdd.glom().map(len).collect())
emp_final = emp.where("salary > 50000")
emp_final.show()
print(emp.schema)
emp_filtered = emp.select(col("employee_id"), expr("name"), emp.age, emp.salary)
emp_filtered.show()
#coloumn typecasting
emp_casted = emp_filtered.select(expr("employee_id as emp_id"), emp.name, expr("cast(age as int) as age"), emp.salary)
emp_casted.show()
emp_casted_1 = emp_filtered.selectExpr("employee_id as emp_id", "name", "cast(age as int) as age", "salary")
emp_casted_1.show()
emp_casted.printSchema()
emp_final = emp_casted.select("emp_id", "name", "age", "salary").where("age > 30")

# Adding Columns
emp_taxed = emp_casted.withColumn("tax", col("salary") * 0.2)
emp_taxed.show()

# Literals
emp_new_cols = emp_taxed.withColumn("columnOne", lit(1)).withColumn("columnTwo", lit('two'))

## Renaming Columns
emp_1 = emp_new_cols.withColumnRenamed("employee_id", "emp_id")

# Column names with Spaces

emp_2 = emp_new_cols.withColumnRenamed("columnTwo", "Column Two")

## Remove Column
emp_dropped = emp_new_cols.drop("columnTwo", "columnOne")

# Filter data 
emp_filtered = emp_dropped.where("tax > 10000")

emp_filtered.show(2)

# Add multiple columns

columns = {
    "tax" : col("salary") * 0.2 ,
    "oneNumber" : lit(1), 
    "columnTwo" : lit("two")
}

emp_final = emp.withColumns(columns)

#####String and Dates######

emp_gender_fixed = emp.withColumn("new_gender", when(col("gender")=='Male', 'M').when(col("gender")=='Female', 'F').otherwise(None))
emp_gender_fixed .show()
#or
emp_gender_fixed_1 = emp.withColumn("new_gender", expr("case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end"))

#Convert Date
emp.show()
emp_date_fixed = emp.withColumn("hire_date", to_date(col("hire_date"),'yyyy-MM-dd'))
emp_date_fixed.printSchema()

emp_dated = emp_date_fixed.withColumn("date_now", current_date()).withColumn("timestamp_now", current_timestamp())



