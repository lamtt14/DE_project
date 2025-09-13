from pyspark.sql import SparkSession

# Tạo SparkSession với Cassandra connector
spark = SparkSession.builder \
    .appName("TestCassandraConnection") \
    .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions") \
    .config("spark.cassandra.connection.host", "my-cassandra") \
    .config("spark.cassandra.connection.port", "9042") \
    .getOrCreate()

# Đọc dữ liệu từ bảng system.local (luôn tồn tại trong Cassandra)
df = spark.read \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="local", keyspace="system") \
    .load()

df.show(5, truncate=False)

spark.stop()
