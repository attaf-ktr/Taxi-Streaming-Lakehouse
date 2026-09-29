from pyspark.sql import SparkSession
import time

# 1. Configuration de Spark (identique pour lire le catalogue)
spark = SparkSession.builder \
    .appName("TaxiLakehouseReader") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "file:///home/kaoutar/projects/streaming-lakehouse/warehouse") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n📊 --- Échantillon brut de la table Bronze ---")
spark.sql("SELECT * FROM local.db.taxi_trips ORDER BY pickup_datetime DESC LIMIT 5").show()

print("\n📈 --- Agrégation SQL sur le Lakehouse ---")
spark.sql("""
    SELECT 
        vendor_id, 
        COUNT(*) as total_trips, 
        ROUND(SUM(total_amount), 2) as total_revenue,
        ROUND(AVG(trip_distance), 2) as avg_distance
    FROM local.db.taxi_trips 
    GROUP BY vendor_id
""").show()

spark.stop()
