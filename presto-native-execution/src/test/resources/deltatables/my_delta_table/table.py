# Dependencies `pip install pyspark delta-spark`

from pyspark.sql import SparkSession
from delta import *

# Create Spark session with Delta Lake
builder = SparkSession.builder.appName("DeltaManifest") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Create sample data
data = [
    (1, "Alice", 34),
    (2, "Bob", 28),
    (3, "Charlie", 42)
]

df = spark.createDataFrame(data, ["id", "name", "age"])

# Write as Delta table
table_path = "/tmp/my_delta_table"
df.write.format("delta").mode("overwrite").save(table_path)

print(f"✓ Delta table created at: {table_path}")

# Generate manifest files
deltaTable = DeltaTable.forPath(spark, table_path)
deltaTable.generate("symlink_format_manifest")

print(f"✓ Manifest generated at: {table_path}/_symlink_format_manifest/")

# Optional: Enable automatic manifest generation for future writes
spark.sql(f"ALTER TABLE delta.`{table_path}` SET TBLPROPERTIES(delta.compatibility.symlinkFormatManifest.enabled=true)")

print("✓ Automatic manifest generation enabled")

spark.stop()