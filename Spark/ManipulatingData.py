
from pyspark.sql import SparkSession

'''Manipulating Columns'''
# Creating Columns
# Create the DataFrame flights
flights = spark.table('flights')
# Show the head
flights.show()
# Add duration_hrs
flights = flights.withColumn('duration_hrs', flights.air_time/60)

'Filtering Data'
# Filter flights by passing a string
long_flights1 = flights.filter('distance > 1000')
# Filter flights by passing a column of boolean values
long_flights2 = flights.filter(flights.distance > 1000)
long_flights1.show()
long_flights2.show()

'Selecting Data'
# Select the first set of columns
selected1 = flights.select("tailnum", "origin", "dest")
# Select the second set of columns
temp = flights.select(flights.origin, flights.dest, flights.carrier)

# Define first filter
filterA = flights.origin == "SEA"
# Define second filter
filterB = flights.dest == "PDX"
# Filter the data, first by filterA then by filterB
selected2 = temp.filter(filterA).filter(filterB)

# Define avg_speed
avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")
# Select the correct columns
speed1 = flights.select("origin", "dest", "tailnum", avg_speed)
# Create the same table using a SQL expression
speed2 = flights.selectExpr("origin", "dest", "tailnum", "distance/(air_time/60) as avg_speed")

'Aggregating'
# Find the shortest flight from PDX in terms of distance
flights.filter(flights.origin == 'PDX').groupBy().min('distance').show()
# Find the longest flight from SEA in terms of air time
flights.filter(flights.origin == 'SEA').groupBy().max('air_time').show()

# Average duration of Delta flights
flights.filter(flights.origin == 'SEA').filter(flights.carrier == 'DL').groupBy().avg('air_time').show()
# Total hours in the air
flights.withColumn('duration_hrs', flights.air_time/60).groupBy().sum('duration_hrs').show()

'Grouping and Aggregating'
# Group by tailnum
by_plane = flights.groupBy("tailnum")
# Number of flights each plane made
by_plane.count().show()
# Group by origin
by_origin = flights.groupBy("origin")
# Average duration of flights from PDX and SEA
by_origin.avg("air_time").show()
"""  
+------+------------------+
|origin|     avg(air_time)|
+------+------------------+
|   SEA| 160.4361496051259|
|   PDX|137.11543248288737|
+------+------------------+ """


import pyspark.sql.functions as F

# Group by month and dest
by_month_dest = flights.groupBy('month', 'dest')
# Average departure delay by month and destination
by_month_dest.avg('dep_delay').show()
# Standard deviation of departure delay
by_month_dest.agg(F.stddev('dep_delay')).show()
"""
Example Result 
+-----+----+----------------------+
|month|dest|stddev_samp(dep_delay)|
+-----+----+----------------------+
|   11| TUS|    3.0550504633038935|
|   11| ANC|    18.604716401245316|
|    1| BUR|     15.22627576540667|
|    1| PDX|     5.677214918493858|
|    6| SBA|     2.380476142847617|
+-----+----+----------------------+
"""

'Joining'

print(airports.show())
"""
+---+--------------------+----------------+-----------------+----+---+---+
|faa|                name|             lat|              lon| alt| tz|dst|
+---+--------------------+----------------+-----------------+----+---+---+
|04G|   Lansdowne Airport|      41.1304722|      -80.6195833|1044| -5|  A|
|06A|Moton Field Munic...|      32.4605722|      -85.6800278| 264| -5|  A|
+---+--------------------+----------------+-----------------+----+---+---+"""

airports = airports.withColumnRenamed('faa', 'dest')
# Join the DataFrames
flights_with_airports = flights.join(airports, on="dest", how="leftouter")
# Examine the new DataFrame
print(flights_with_airports.show())

"""
+----+--------------------+----------------+-----------------+----+---+---+
|dest|                name|             lat|              lon| alt| tz|dst|
+----+--------------------+----------------+-----------------+----+---+---+
| 04G|   Lansdowne Airport|      41.1304722|      -80.6195833|1044| -5|  A|
| 06A|Moton Field Munic...|      32.4605722|      -85.6800278| 264| -5|  A|
| 06C| Schaumburg Regional|      41.9893408|      -88.1012428| 801| -6|  A|
| 06N|     Randall Airport|       41.431912|      -74.3915611| 523| -5|  A|
+----+--------------------+----------------+-----------------+----+---+---+ """