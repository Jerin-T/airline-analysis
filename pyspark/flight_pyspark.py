from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import sys

# Create a Spark session
spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()

# Load the flight data into a DataFrame (assuming you have a 'flight' DataFrame)
input_dir = sys.argv[1]
# Load CSV files into DataFrames
flight = spark.read.csv(input_dir+"flights.csv", header=True, inferSchema=True)

# Calculate average departure and arrival delay using PySpark functions
result = flight.groupBy("year", "month", "airline") \
               .agg(avg("departure_delay").alias("avg_departure_delay"),
                    avg("arrival_delay").alias("avg_arrival_delay")) \
               .orderBy("year", "month", "airline")

# Show the result
result.show()
print(result)

# Stop the Spark session
spark.stop()

SELECT
    year,
    month,
    airline,
    AVG(departure_delay) AS avg_departure_delay,
    AVG(arrival_delay) AS avg_arrival_delay
FROM
    flight
GROUP BY
    year,
    month,
    airline
ORDER BY
    year, month, airline;
