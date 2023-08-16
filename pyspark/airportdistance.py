import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import asin, cos, pow, sin, sqrt, col
from math import pi
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("AirportDistanceAnalysis").getOrCreate()

input_dir = sys.argv[1]
airports_df = spark.read.csv(input_dir + "airports.csv", header=True)
airports_df.createOrReplaceTempView("airports")

distance_threshold_in_km = 100.0
result = airports_df.alias("a1").join(
    airports_df.alias("a2"),
    col("a1.IATA_CODE") != col("a2.IATA_CODE") 
).where(
    6371 * 2 * asin(sqrt(
        pow(sin((col("a2.LATITUDE") - col("a1.LATITUDE")) * pi / 180 / 2), 2) +
        cos(col("a1.LATITUDE") * pi / 180) *
        cos(col("a2.LATITUDE") * pi / 180) *
        pow(sin((col("a2.LONGITUDE") - col("a1.LONGITUDE")) * pi / 180 / 2), 2)
    )) <= distance_threshold_in_km
).select(
    col("a1.IATA_CODE").alias("airport1"),  
    col("a2.IATA_CODE").alias("airport2"),  
    (6371 * 2 * asin(sqrt(
        pow(sin((col("a2.LATITUDE") - col("a1.LATITUDE")) * pi / 180 / 2), 2) +
        cos(col("a1.LATITUDE") * pi / 180) *
        cos(col("a2.LATITUDE") * pi / 180) *
        pow(sin((col("a2.LONGITUDE") - col("a1.LONGITUDE")) * pi / 180 / 2), 2)
    ))).cast(DoubleType()).alias("distance_in_km")
).orderBy(
    col("airport1"), col("airport2")
)
result.show()
spark.stop()
