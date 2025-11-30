
# ====================================================
# MySQL Configuration
# ====================================================

MYSQL_HOST = "217.61.57.46"
MYSQL_PORT = "3306"
MYSQL_DB = "olympic_dataset"
MYSQL_USER = "neo_data_admin"
MYSQL_PASSWORD = "Proyahaxuqithab9oplp"

# JDBC URL
JDBC_URL = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

# MySQL Tables
TABLE_ATHLETE_BIO = "athlete_bio"
TABLE_ATHLETE_EVENTS = "athlete_event_results"
TABLE_OUTPUT = "enriched_athlete_avg"


# =====================================================
# Kafka Configuration
# =====================================================

KAFKA_BOOTSTRAP_SERVERS = "77.81.230.104:9092"
KAFKA_ATHLETE_EVENTS = "athlete_event_results"
KAFKA_OUTPUT_TOPIC = "enriched_athlete_avg"
KAFKA_USERNAME = "admin"
KAFKA_PASSWORD = "VawEzo1ikLtrA8Ug8THa"
KAFKA_JAAS_CONFIG = (
    'org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{KAFKA_USERNAME}" password="{KAFKA_PASSWORD}";'
)


# ====================================================
# Spark Configuration
# ====================================================

SPARK_APP_NAME = "OlympicStreamingPipeline"
CHECKPOINT_LOCATION = "checkpoint/olympic_streaming"
PROCESSING_TIME = "30 seconds"
MAX_OFFSETS_PER_TRIGGER = 100

# ====================================================
# File Paths
# ====================================================
MYSQL_CONNECTOR_JAR = "mysql-connector-j-8.0.32.jar"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2"

