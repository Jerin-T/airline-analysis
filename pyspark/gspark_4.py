from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Initialize a Spark session
spark = SparkSession.builder.appName("FlightAnalysis").getOrCreate()
input_dir = sys.argv[1]

# Load data and define schemas
flight_df = spark.read.csv(input_dir + "flights.csv", header=True)
airlines_df = spark.read.csv(input_dir + "airlines.csv", header=True)

result_df = flight_df.join(airlines_df, flight_df["airline"] == airlines_df["iata_code"]) \
    .where(col("arrival_delay") > col("departure_delay")) \
    .select(airlines_df["airline"], flight_df["flight_number"], flight_df["arrival_delay"], flight_df["departure_delay"]) \
    .orderBy((col("arrival_delay") - col("departure_delay")).desc())

# Show the results
result_df.show()
