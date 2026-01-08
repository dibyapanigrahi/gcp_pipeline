# Spark Session
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("Run Concurrent/Parallel task in Spark")
    .master("spark://197e20b418a6:7077")
    .config("spark.cores.max", 8)
    .config("spark.executor.cores", 4)
    .config("spark.executor.memory", "512M")
    .getOrCreate()
)

def extract_country_data(_country: str):
    try:
        # Read Cities data
        df_cities = (
            spark
            .read
            .format("csv")
            .option("header", True)
            .load("/data/input/cities.csv")
        )

        # Fiter data
        df_final = df_cities.where(f"lower(country) = lower('{_country}')")

        # Write data
        (
            df_final
            .coalesce(1)
            .write
            .format("csv")
            .mode("overwrite")
            .option("header", True)
            .save(f"/data/output/countries/{_country.lower()}/")
        )
    
        return f"Data Extracted for {_country} at: [/data/output/countries/{_country.lower()}/]"
    except Exception as e:
        raise Exception(e)
    
    # Use For loops to execute the jobs
import time

# Set start time
start_time = time.time()

# Run all extracts through for-loop
_countries = ['India', 'Iran', 'Ghana', 'Australia']

for _country in _countries:
    print(extract_country_data(_country))

# End time
end_time = time.time()

# Total time taken
print(f"total time = {end_time - start_time} seconds")

# Use threads to run the queries in concurrently/parallely
import time
import concurrent.futures

# Set start time
start_time = time.time()

_countries = ['India', 'Iran', 'Ghana', 'Australia']

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = {executor.submit(extract_country_data, _country) for _country in _countries}
    
    for result in results:
        print(result.result())
        
# End time
end_time = time.time()

# Total time taken
print(f"total time = {end_time - start_time} seconds")

##
# Data Extracted for Iran at: [/data/output/countries/iran/]
# Data Extracted for India at: [/data/output/countries/india/]
# Data Extracted for Ghana at: [/data/output/countries/ghana/]
# Data Extracted for Australia at: [/data/output/countries/australia/]
# total time = 5.513016700744629 seconds