#  Perform analytics using data from YugabyteDB and save results back to YugabyteDB

import logging
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType
from engine.spark import create_spark_session
from engine.yugabyte import (
    get_jdbc_url,
    get_jdbc_properties,
    create_analytics_tables
)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def perform_analytics():
    logger.info("Starting analytics processing...")

    # Create Spark session
    spark = create_spark_session()

    # Get JDBC connection parameters
    jdbc_url = get_jdbc_url()
    jdbc_properties = get_jdbc_properties()

    # Read airports data from YugabyteDB
    logger.info("Reading airports data from YugabyteDB...")
    airports_df = spark.read.jdbc(url=jdbc_url, table="airports", properties=jdbc_properties)

    # Read airlines data from YugabyteDB
    logger.info("Reading airlines data from YugabyteDB...")
    airlines_df = spark.read.jdbc(url=jdbc_url, table="airlines", properties=jdbc_properties)

    # Perform analytics operations
    logger.info("Performing analytics...")

    # Example 1: Count number of airports per country
    logger.info("Calculating the number of airports per country...")
    airports_per_country = airports_df.groupBy("country").count().orderBy(col("count").desc())
    airports_per_country.show(10)

    # Example 2: Count number of airlines per country
    logger.info("Calculating the number of airlines per country...")
    airlines_per_country = airlines_df.groupBy("country").count().orderBy(col("count").desc())
    airlines_per_country.show(10)

    # Example 3: Join airlines and airports on country and perform analysis
    logger.info("Joining airlines and airports data on country...")
    joined_df = airlines_df.join(airports_df, on="country", how="inner")

    # Example 4: Calculate the average number of airlines per airport in each country
    logger.info("Calculating the average number of airlines per airport in each country...")
    airlines_count = airlines_df.groupBy("country").count().withColumnRenamed("count", "airlines_count")
    airports_count = airports_df.groupBy("country").count().withColumnRenamed("count", "airports_count")
    avg_airlines_per_airport = airlines_count.join(airports_count, on="country") \
        .withColumn("avg_airlines_per_airport", col("airlines_count") / col("airports_count")) \
        .orderBy(col("avg_airlines_per_airport").desc())
    avg_airlines_per_airport.show(10)

    # Save analytics results back to YugabyteDB
    logger.info("Saving analytics results to YugabyteDB...")

    # Create analytics tables if they don't exist
    create_analytics_tables()

    # Prepare DataFrames for writing (cast columns to appropriate types)
    airports_per_country = airports_per_country \
        .withColumn("count", col("count").cast(IntegerType()))

    airlines_per_country = airlines_per_country \
        .withColumn("count", col("count").cast(IntegerType()))

    avg_airlines_per_airport = avg_airlines_per_airport \
        .withColumn("airlines_count", col("airlines_count").cast(IntegerType())) \
        .withColumn("airports_count", col("airports_count").cast(IntegerType())) \
        .withColumn("avg_airlines_per_airport", col("avg_airlines_per_airport").cast(DoubleType()))

    # Write the results back to YugabyteDB
    airports_per_country.write.jdbc(
        url=jdbc_url,
        table="airports_per_country",
        mode="overwrite",
        properties=jdbc_properties
    )

    airlines_per_country.write.jdbc(
        url=jdbc_url,
        table="airlines_per_country",
        mode="overwrite",
        properties=jdbc_properties
    )

    avg_airlines_per_airport.write.jdbc(
        url=jdbc_url,
        table="avg_airlines_per_airport",
        mode="overwrite",
        properties=jdbc_properties
    )

    logger.info("Analytics processing completed.")

if __name__ == "__main__":
    logger.info("Analytics script started.")
    perform_analytics()
    logger.info("Analytics script completed.")
