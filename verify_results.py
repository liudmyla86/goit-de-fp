"""
Script to check streaming pipeline results
Checks data in MySQL table and Kafka topic
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Configuration
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"
kafka_bootstrap_servers = "77.81.230.104:9092"
output_topic = "enriched_athlete_avg"

# Kafka SASL/PLAIN Authentication
kafka_username = "admin"
kafka_password = "VawEzo1ikLtrA8Ug8THa"
kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_username}" password="{kafka_password}";'
)

# Creating a Spark session
spark = SparkSession.builder \
    .appName("VerifyResults") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 80)
print("CHECKING THE RESULTS STREAMING PIPELINE")
print("=" * 80)

# ============================================================================
# Checking data in a MySQL table
# ============================================================================
print("\n1. Checking data in a MySQL table 'enriched_athlete_avg'")
print("=" * 80)

try:
    db_results = spark.read.format('jdbc').options(
        url=jdbc_url,
        driver='com.mysql.cj.jdbc.Driver',
        dbtable='enriched_athlete_avg',
        user=jdbc_user,
        password=jdbc_password
    ).load()
    
    total_records = db_results.count()
    print(f"\nFound {total_records} records in table")
    
    if total_records > 0:
        print("\nFirst 30 entries:")
        db_results.orderBy(col("timestamp").desc()).show(30, truncate=False)
        
        print("\nTable diagram:")
        db_results.printSchema()
        
        print("\nStatistics by sport:")
        db_results.groupBy("sport").count().orderBy(col("count").desc()).show(20)
        
        print("\nMedal statistics:")
        db_results.groupBy("medal").count().show()
        
        print("\nExamples of aggregated data (sorted by avg_height):")
        db_results.orderBy(col("avg_height").desc()).show(10, truncate=False)
        
    else:
        print(" The table is empty.")
        
except Exception as e:
    print(f"Error reading from MySQL: {e}")

# ============================================================================
# Validating data in a Kafka topic
# ============================================================================
print("\n" + "=" * 80)
print(f"2. VALIDATING DATA IN A KAFKA TOPIC '{output_topic}'")
print("=" * 80)

try:
    # Schema for data from Kafka
    kafka_schema = StructType([
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("country_noc", StringType(), True),
        StructField("avg_height", DoubleType(), True),
        StructField("avg_weight", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    # Reading a topic from Kafka
    kafka_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", output_topic) \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", kafka_jaas_config) \
        .load()
    
    # Convert JSON to columns
    kafka_data = kafka_df.select(
        from_json(col("value").cast("string"), kafka_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition"),
        col("offset")
    ).select("data.*", "kafka_timestamp", "partition", "offset")
    
    total_messages = kafka_data.count()
    print(f"\n Found {total_messages} messages in Kafka topic")
    
    if total_messages > 0:
        print(f"\nFirst 30 posts from the topic '{output_topic}':")
        kafka_data.orderBy(col("kafka_timestamp").desc()).show(30, truncate=False)
        
        print("\nData schema:")
        kafka_data.printSchema()
        
        print("\nPartitioning:")
        kafka_data.groupBy("partition").count().orderBy("partition").show()
        
        print("\nLast 10 messages (by offset):")
        kafka_data.orderBy(col("offset").desc()).show(10, truncate=False)
        
    else:
        print(f"Topic '{output_topic}' is empty or does not exist.")
        
except Exception as e:
    print(f" Error reading from Kafka: {e}")

print("\n" + "=" * 80)
print("VERIFICATION COMPLETED")
print("=" * 80 + "\n")

spark.stop()