from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import sys

spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()
input_dir = sys.argv[1]
flight = spark.read.csv(input_dir+"flights.csv", header=True, inferSchema=True)

result = flight.groupBy("year", "month", "airline") \
               .agg(avg("departure_delay").alias("avg_departure_delay"),
                    avg("arrival_delay").alias("avg_arrival_delay")) \
               .orderBy("year", "month", "airline")

result.show()
print(result)

spark.stop()