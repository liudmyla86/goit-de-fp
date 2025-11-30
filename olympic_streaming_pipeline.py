from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import to_json, struct

# MySqlL credentials
jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "neo_data_admin"
jdbc_password = "Proyahaxuqithab9oplp"

# Kafka configuration
kafka_bootstrap_servers = "77.81.230.104:9092"
input_topic = "athlete_event_results"
output_topic = "enriched_athlete_avg"

# Kafka SASL/PLAIN Authentication
kafka_username = "admin"
kafka_password = "VawEzo1ikLtrA8Ug8THa"
kafka_jaas_config = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{kafka_username}" password="{kafka_password}";'
)

# Creating Spark session with Kafka and MySQL support
spark = SparkSession.builder \
    .appName("OlympicStreamingPipeline") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.jars", "mysql-connector-j-8.0.32.jar") \
    .config("spark.sql.streaming.checkpointLocation", "checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Streaming Pipeline launched")
print("=" * 80)

# =====================================================================
# Stage 1: Reading athlete physical performance data from MySQL table
# =====================================================================
print("\nStage 1: Reading athlete_bio from MySQL...")

athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='athlete_bio',
    user=jdbc_user,
    password=jdbc_password
).load()

print(f"Loaded {athlete_bio_df.count()} records from athlete_bio")
athlete_bio_df.show(5)
athlete_bio_df.printSchema()


# =====================================================================
# Stage 2: Data filtering - removing records with empty height/weight
# =====================================================================
print("\nStage 2: Filtering data with empty or non-numeric values...")

# Filtering records where height and weight not null and are numbers
athlete_bio_clean = athlete_bio_df.filter(
    (col("height").isNotNull()) &
    (col("weight").isNotNull()) &
    (col("height").cast("double").isNotNull()) &
    (col("weight").cast("double").isNotNull())
)

print(f"After filtration, what remained was {athlete_bio_clean.count()} records")
athlete_bio_clean.show(5)


# =====================================================================
# Stage 3: Reading athlete_event_results and writing to Kafka
# =====================================================================
print("\nStage 3: Reading athlete_event_results and writing to Kafka...")

# Reading table athlete_event_results
athlete_events_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable='athlete_event_results',
    user=jdbc_user,
    password=jdbc_password
).load()

print(f"Loaded {athlete_events_df.count()} records from athlete_event_results")
athlete_events_df.show(5)

kafka_write_df = athlete_events_df.select(
    to_json(struct("*")).alias("value")
)

# Write data to Kafka topic athlete_event_results
print(f"Write data to Kafka topic: {input_topic}")
kafka_write_df.write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", input_topic) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", kafka_jaas_config) \
    .save()

print("Data successfully written to Kafka topic")


# =====================================================================
# Stage 3 (continued): Reading data from a Kafka topic
# =====================================================================
print(f"\nReading streaming data from Kafka topic: {input_topic}...")

# Defining a schema for JSON data from Kafka
kafka_schema = StructType([
    StructField("athlete_id", IntegerType(), True),
    StructField("sport", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result", StringType(), True),
    StructField("country_noc", StringType(), True)

])

# Reading streaming data from Kafka
kafka_stream_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 100) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", kafka_jaas_config) \
    .load()

# Convert JSON into separate columns
event_stream_df = kafka_stream_df.select(
    from_json(col("value").cast("string"), kafka_schema).alias("data")
).select("data.*")

print("Streaming with Kafka configured")


# =====================================================================
# Stage 4: Merging data from Kafka with biological data from MySQL
# =====================================================================
print("\nStage 4: Merging data from Kafka with biological data ...")

# Join streaming data from athlete events and static bio data by athlete_id
enriched_stream_df = event_stream_df.join(
    athlete_bio_clean,
    event_stream_df.athlete_id == athlete_bio_clean.athlete_id,
    "inner"
).select(
    event_stream_df.athlete_id,
    event_stream_df.sport,
    event_stream_df.medal,
    event_stream_df.country_noc,
    athlete_bio_clean.sex,
    col("height").cast("double").alias("height"),
    col("weight").cast("double").alias("weight")
)

print("Stream enriched with biological data")


# ====================================================================
# Stage 5: Calculation of average height and weight
# ====================================================================
print("\nStage 5: Calculating aggregate metrics...")

# Group by sport, medal, gender and country
# Calculate average height and weight values
aggregated_stream_df = enriched_stream_df.groupBy(
    "sport",
    "medal",
    "sex",
    "country_noc"
).agg(
    avg("height").alias("avg_height"),
    avg("weight").alias("avg_weight"),
    current_timestamp().alias("timestamp")
)

print("Aggregation is configured")


# =================================================================
# Stage 6: Recording results (forEachBatch pattern - FanOut)
# =================================================================
print("\nStage 6: Setting up results recording...")

def foreach_batch_function(batch_df, batch_id):
    """
    Function to process each microbatch
    Implements FanOut pattern: write to Kafka and MySQL simultaneously
    """
    print(f"\n{'=' * 80}")
    print(f"Process batch_id: {batch_id}")
    print(f"{'=' * 80}")


    # Showing data for verification
    print(f"Data in the batch (first 20 lines):")
    batch_df.show(20, truncate=False)
    print(f"Number of records: {batch_df.count()}")

    try:
        # ================================================   
        # Stage 6.a): Writing to the original Kafka topic
        # ================================================
        print(f"\nStage6.a): writing to thr Kafka-topic '{output_topic}'...")

        kafka_output_df = batch_df.select(
            to_json(struct("*")).alias("value")
        )

        kafka_output_df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", output_topic) \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", kafka_jaas_config) \
            .save()
        print(f"Batch {batch_id} written to Kafka topic '{output_topic}'")
    
    except Exception as e:
        print(f"Error writing to Kafka: {e}")

    
    try:
        # =================================================
        # Stage 6.b): Writing to a MySQL database
        # =================================================
        print(f"\nStage 6.b): Writing to MySQL table 'enriched_athlete_avg'...")

        batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "enriched_athlete_avg") \
            .option("user", jdbc_user) \
            .option("password", jdbc_password) \
            .mode("append") \
            .save()
        
        print(f"Batch {batch_id} written to MySQL table 'enriched_athlete_avg'")

    except Exception as e:
        print(f"Error writing to MySQL: {e}")

    print(f"{'=' * 80}\n")

# Starting streaming using forEachBatch
print("\n" + "=" * 80)
print("STARTING STREAMING PIPELINE")
print("=" * 80)
print(f"Input topic: {input_topic}")
print(f"Output topic: {output_topic}")
print(f"Database table: enriched_athlete_avg")
print("=" * 80)

query = aggregated_stream_df \
    .writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("complete") \
    .option("checkpointLocation", "checkpoint/olympic_streaming") \
    .trigger(processingTime='30 seconds') \
    .start()

print("\nStreaming pipeline is active. Waiting for data...")
print("Press Ctrl+C to stop\n")

# Awaiting completion
query.awaitTermination()
        
        
