
'''Create a SQL table from a dataframe'''

# Load trainsched.txt
df = spark.read.csv("trainsched.txt", header=True)
# Create temporary table called table1
df.createOrReplaceTempView('table1')

# Inspect the columns in the table df
spark.sql("DESCRIBE schedule").show()
'''
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|train_id|   string|   null|
| station|   string|   null|
|    time|   string|   null|
+--------+---------+-------+ '''

'''window function SQL'''
# Add col running_total that sums diff_min col in each group
query = """
    SELECT train_id, station, time, diff_min,
    SUM(diff_min) OVER (PARTITION BY train_id ORDER BY time) AS running_total
    FROM schedule
    """
# Run the query and display the result
spark.sql(query).show()
'''
    +--------+-------------+-----+--------+-------------+
    |train_id|      station| time|diff_min|running_total|
    +--------+-------------+-----+--------+-------------+
    |     217|       Gilroy|6:06a|     9.0|          9.0|
    |     217|   San Martin|6:15a|     6.0|         15.0|
    |     217|  Morgan Hill|6:21a|    15.0|         30.0|
    |     217| Blossom Hill|6:36a|     6.0|         36.0|
    |     217|      Capitol|6:42a|     8.0|         44.0|
    |     217|       Tamien|6:50a|     9.0|         53.0|
    |     217|     San Jose|6:59a|    null|         53.0|
    |     324|San Francisco|7:59a|     4.0|          4.0|
    |     324|  22nd Street|8:03a|    13.0|         17.0|
    |     324|     Millbrae|8:16a|     8.0|         25.0|
    |     324|    Hillsdale|8:24a|     7.0|         32.0|
    |     324| Redwood City|8:31a|     6.0|         38.0|
    |     324|    Palo Alto|8:37a|    28.0|         66.0|
    |     324|     San Jose|9:05a|    null|         66.0|
    +--------+-------------+-----+--------+-------------+ '''

'Fixed Broken Query'
query = """  (1)
SELECT (2)
ROW_NUMBER() OVER (ORDER BY time) AS row, (3)
train_id, (4)
station, (5)
time, (6)
LEAD(time,1) OVER (ORDER BY time) AS time_next (7)
FROM schedule (8)
""" (9)
spark.sql(query).show()
# Give the number of the bad row as an integer
bad_row = 7 # row number 7 missing the clause
# Provide the missing clause, SQL keywords in upper case
clause = 'PARTITION BY train_id'

'''Dot notation and SQL'''
'Aggregation'
# Give the identical result in each command
spark.sql('SELECT train_id, MIN(time) AS start FROM schedule GROUP BY train_id').show()
df.groupBy('train_id').agg({'time':'min'}).withColumnRenamed('min(time)', 'start').show()

# Print the second column of the result
spark.sql('SELECT train_id, MIN(time), MAX(time) FROM schedule GROUP BY train_id').show()
result = df.groupBy('train_id').agg({'time':'min', 'time':'max'})
result.show()
print(result.columns[1])

'''
+--------+-----+
|train_id|start|
+--------+-----+
|     217|6:06a|
|     324|7:59a|
+--------+-----+

+--------+---------+---------+
|train_id|min(time)|max(time)|
+--------+---------+---------+
|     217|    6:06a|    6:59a|
|     324|    7:59a|    9:05a|
+--------+---------+---------+

+--------+---------+
|train_id|max(time)|
+--------+---------+
|     217|    6:59a|
|     324|    9:05a|
+--------+---------+ '''

'Aggregating the same column twice'
from pyspark.sql.functions import min, max, col
expr = [min(col("time")).alias('start'), max(col("time")).alias('end')]
dot_df = df.groupBy("train_id").agg(*expr)
dot_df.show()

# Write a SQL query giving a result(as above) identical to dot_df 
query = "SELECT train_id, MIN(time) AS start, MAX(time) AS end FROM schedule GROUP BY train_id"
sql_df = spark.sql(query)
sql_df.show()

'''
+--------+-----+-----+
|train_id|start|  end|
+--------+-----+-----+
|     217|6:06a|6:59a|
|     324|7:59a|9:05a|
+--------+-----+-----+ '''

'Aggregate dot SQL'
df = spark.sql("""
SELECT *, 
LEAD(time,1) OVER(PARTITION BY train_id ORDER BY time) AS time_next 
FROM schedule
""")

from pyspark.sql import Window
from pyspark.sql.functions import row_number
# Obtain the identical result using dot notation 
dot_df = df.withColumn('time_next', lead('time', 1)
        .over(Window.partitionBy('train_id')
        .orderBy('time')))

'Convert window function from dot notation to SQL'
window = Window.partitionBy('train_id').orderBy('time')
dot_df = df.withColumn('diff_min', 
                    (unix_timestamp(lead('time', 1).over(window),'H:m') 
                     - unix_timestamp('time', 'H:m'))/60)
# Create a SQL query to obtain an identical result to dot_df (result same as above query)
query = """
SELECT *, 
(unix_timestamp(lead(time, 1) OVER (PARTITION BY train_id ORDER BY time),'H:m') 
 - unix_timestamp(time, 'H:m'))/60 AS diff_min 
FROM schedule 
"""
sql_df = spark.sql(query)
sql_df.show()
'''
    +--------+-------------+-----+--------+
    |train_id|      station| time|diff_min|
    +--------+-------------+-----+--------+
    |     217|       Gilroy|6:06a|     9.0|
    |     217|   San Martin|6:15a|     6.0|
    |     217|  Morgan Hill|6:21a|    15.0|
    |     217| Blossom Hill|6:36a|     6.0|
    |     217|      Capitol|6:42a|     8.0|
    |     217|       Tamien|6:50a|     9.0|
    |     217|     San Jose|6:59a|    null|
    |     324|San Francisco|7:59a|     4.0|
    |     324|  22nd Street|8:03a|    13.0|
    |     324|     Millbrae|8:16a|     8.0|
    |     324|    Hillsdale|8:24a|     7.0|
    |     324| Redwood City|8:31a|     6.0|
    |     324|    Palo Alto|8:37a|    28.0|
    |     324|     San Jose|9:05a|    null|
    +--------+-------------+-----+--------+ '''


