import logging
import random

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='/opt/airflow/logs/spark_job.log')
logger = logging.getLogger()


def calculate_pi(partitions):
    def inside(_):
        x, y = random.random(), random.random()
        return x*x + y*y < 1

    count = spark.sparkContext.parallelize(range(1, partitions + 1), partitions).filter(inside).count()
    return 4.0 * count / partitions

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("CalculatePi").getOrCreate()

    # Number of partitions (parallel tasks)
    partitions = 100
    logger.log('partitions:' + partitions)
    # Calculate Pi
    pi_value = calculate_pi(partitions)
    logger.log('pi_value:' + pi_value)
    # Print the result
    print(f"Pi is roughly {pi_value}")

    # Stop the Spark session
    spark.stop()
