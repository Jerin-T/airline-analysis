from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys
# Initialize a Spark session
spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()
input_dir = sys.argv[1]
# Load data and define schemas
flight_df = spark.read.csv(input_dir + "flights.csv", header=True)
airlines_df = spark.read.csv(input_dir + "airlines.csv", header=True)
# Perform analysis
result_df = flight_df.join(airlines_df, flight_df["airline"] == airlines_df["iata_code"]) \
    .where(col("departure_delay") < 0) \
    .select(airlines_df["airline"], flight_df["flight_number"], flight_df["scheduled_departure"], flight_df["departure_delay"]) \
    .orderBy((flight_df["scheduled_departure"] - flight_df["departure_delay"]).asc()) \
    .limit(10)


# Show the results
result_df.show()

# Stop the Spark session
spark.stop()



