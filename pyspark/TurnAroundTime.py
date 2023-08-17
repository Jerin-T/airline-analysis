import sys
from pyspark.sql import SparkSession

input_dir = sys.argv[1]
spark = SparkSession.builder.appName("avg_dept_delay_by_month").getOrCreate()

flight_df = spark.read.csv(input_dir + "flights.csv", header=True)
airports_df = spark.read.csv(input_dir + "airports.csv", header=True)
airlines_df = spark.read.csv(input_dir + "airlines.csv", header=True)
cancellation_df = spark.read.csv(input_dir + "cancellation.csv", header=True)

airlines_df.createOrReplaceTempView("airlines")
airports_df.createOrReplaceTempView("airports")
cancellation_df.createOrReplaceTempView("cancellation")
flight_df.createOrReplaceTempView("flight")
result = spark.sql("""
    SELECT airline_name, flight_number, MIN(arrival_time - departure_time) AS shortest_turnaround_time
    FROM flight
    WHERE departure_time IS NOT NULL AND arrival_time IS NOT NULL
    GROUP BY airline_name, flight_number
    ORDER BY shortest_turnaround_time ASC
    LIMIT 10
""")

result.show()
spark.stop()