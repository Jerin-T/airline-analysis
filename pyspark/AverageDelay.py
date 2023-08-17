import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
spark = SparkSession.builder.appName("HighDelayFlightsAnalysis").getOrCreate()
input_dir = sys.argv[1]
flight_df = spark.read.csv(input_dir + "flights.csv", header=True)
flight_df.createOrReplaceTempView("flight")
average_departure_delay = flight_df.select(avg(col("departure_delay"))).collect()[0][0]
average_arrival_delay = flight_df.select(avg(col("arrival_delay"))).collect()[0][0]
result = flight_df.select(
    col("airline"), col("flight_number"), col("departure_delay"), col("arrival_delay")
).where(
    (col("departure_delay") > average_departure_delay)
    & (col("arrival_delay") > average_arrival_delay)
).orderBy(
    col("departure_delay").desc(), col("arrival_delay").desc()
).limit(10)
result.show()

spark.stop()