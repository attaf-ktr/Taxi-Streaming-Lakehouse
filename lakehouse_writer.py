from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

# 1. Configuration de Spark avec les extensions Apache Iceberg
spark = SparkSession.builder \
    .appName("TaxiStreamingLakehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "file:///home/kaoutar/projects/streaming-lakehouse/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Création du Namespace et de la table Iceberg
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")

spark.sql("""
    CREATE TABLE IF NOT EXISTS local.db.taxi_trips (
        trip_id INT,
        vendor_id INT,
        pickup_datetime TIMESTAMP,
        passenger_count INT,
        trip_distance DOUBLE,
        total_amount DOUBLE
    ) USING iceberg
""")

# 3. Schéma des données entrantes
schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("vendor_id", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

# 4. Lecture du flux Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "taxi-trips-stream") \
    .option("startingOffsets", "latest") \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 5. Écriture du flux en continu au format Iceberg
query = parsed_stream.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("path", "local.db.taxi_trips") \
    .option("checkpointLocation", "file:///home/kaoutar/projects/streaming-lakehouse/checkpoints/taxi_trips") \
    .start()

print("🌊 Écriture du flux en continu vers Apache Iceberg (Dossier 'warehouse')...")
query.awaitTermination()