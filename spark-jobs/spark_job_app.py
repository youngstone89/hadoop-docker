import logging
import random

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

def calculate_pi(spark, partitions):
    def inside(_):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    count = spark.sparkContext.parallelize(range(1, partitions + 1), partitions).filter(inside).count()
    return 4.0 * count / partitions

if __name__ == "__main__":
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("CalculatePi") \
            .master("spark://spark-master:7077") \
            .getOrCreate()

        logger.info("Spark session created successfully")

        # Number of partitions (parallel tasks)
        partitions = 100
        
        # Calculate Pi
        pi_value = calculate_pi(spark, partitions)
        
        # Log and print the result
        result_message = f"Pi is roughly {pi_value}"
        logger.info(result_message)
        print(result_message)

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        # Stop the Spark session
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")
