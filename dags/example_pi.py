from pyspark.sql import SparkSession
import random

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySparkPi").getOrCreate()
    sc = spark.sparkContext

    num_samples = 100000
    count = sc.parallelize(range(0, num_samples)).filter(
        lambda _: (random.random() ** 2 + random.random() ** 2) < 1
    ).count()

    print(f"Pi is roughly {4.0 * count / num_samples}")
    spark.stop()