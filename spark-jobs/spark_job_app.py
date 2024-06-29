import logging
import random
import sys

from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_point_inside_unit_circle(_):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y <= 1 else 0

def calculate_pi(spark, num_samples=1000000):
    try:
        # Create a DataFrame with a range of numbers
        df = spark.range(0, num_samples)
        
        # Apply the function to check if points are inside the unit circle
        count = df.rdd.map(is_point_inside_unit_circle).reduce(lambda a, b: a + b)
        
        # Calculate Pi
        pi = 4 * count / num_samples
        return pi
    except Exception as e:
        logger.error(f"Error in Pi calculation: {str(e)}")
        raise

def main():
    spark = None
    try:
        logger.info("Initializing Spark session")
        spark = SparkSession.builder \
            .appName("CalculatePi") \
            .master("spark://spark-master:7077") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.network.timeout", "600s") \
            .getOrCreate()

        logger.info("Spark session created successfully")

        pi = calculate_pi(spark)
        logger.info(f"Pi is roughly {pi}")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        sys.exit(1)
    finally:
        if spark:
            logger.info("Stopping Spark session")
            spark.stop()

if __name__ == "__main__":
    main()
