
from pyspark.sql import SparkSession

'Introduction PySpark'
# Creating a SparkSession
my_spark = SparkSession.builder.getOrCreate()
# Viewing tables
print(spark.catalog.listTables())

#Run Query 
query = "FROM flights SELECT * LIMIT 10" # Get the first 10 rows of flights
flights10 = spark.sql(query)
# Show the results
flights10.show()

query = "SELECT origin, dest, COUNT(*) as N FROM flights GROUP BY origin, dest"
flight_counts = spark.sql(query)
# Convert the results to a pandas DataFrame
pd_counts = flight_counts.toPandas()
# Print the head of pd_counts
print(pd_counts.head())

# Create pd_temp
pd_temp = pd.DataFrame(np.random.random(10))
# Create spark_temp from pd_temp
spark_temp = spark.createDataFrame(pd_temp)
# Add spark_temp to the catalog, Register spark_temp as a temporary table named "temp" using the .createOrReplaceTempView() method
spark_temp.createOrReplaceTempView("temp")
print(spark.catalog.listTables())


file_path = "/usr/local/share/datasets/airports.csv"
# Read in the airports data
airports = spark.read.csv(file_path, header = True)
airports.show()



