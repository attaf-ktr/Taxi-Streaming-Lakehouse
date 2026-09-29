from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# 1. Initialisation de Spark
spark = SparkSession.builder \
    .appName("TaxiStreamingConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Définition du schéma exact de notre Producteur
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

print("🚀 Connexion au flux Kafka...")

# 3. Lecture du flux Kafka en continu
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi-trips-stream") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parsing du JSON
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Transformation Stateful (Watermarking & Aggrégation par fenêtre)
aggregated_stream = parsed_stream \
    .withWatermark("pickup_datetime", "10 minutes") \
    .groupBy(
        window(col("pickup_datetime"), "1 minute")
    ) \
    .agg(
        {"total_amount": "sum", "trip_distance": "avg"}
    )

# 6. Écriture dans la console (pour vérification avant Iceberg)
query = aggregated_stream.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
