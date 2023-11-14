import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType,BooleanType, IntegerType, ArrayType

SparkSession.builder.config(conf=SparkConf())

spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .getOrCreate()


df = spark.readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "movie_data") \
  .option("startingOffsets", "earliest") \
  .load()


value_df = df.selectExpr("CAST(value AS STRING)")

# StructType for themoviedb.org

schema = StructType([
    StructField("adult", BooleanType(), True),
    StructField("belongs_to_collection", StructType([
        StructField("name", StringType(), True),
        StructField("poster_path", StringType(), True),
        StructField("backdrop_path", StringType(), True)
    ]), True),
    StructField("budget", IntegerType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("production_companies", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("logo_path", StringType(), True),
        StructField("name", StringType(), True),
        StructField("origin_country", StringType(), True)
    ])), True),
    StructField("production_countries", ArrayType(StructType([
        StructField("iso_3166_1", StringType(), True),
        StructField("name", StringType(), True)
    ])), True),
    StructField("release_date", StringType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", StringType(), True),
    StructField("vote_count", IntegerType(), True)
])


selected_df = value_df.withColumn("values", F.from_json(value_df["value"], schema)).selectExpr("values")

result_df = selected_df.select(
    F.col("values.status").alias("status"),
    F.col("values.overview").alias("overview"),
    F.col("values.production_companies.name").alias("name_production_company"),
    F.col("values.original_language").alias("original_language"),
    F.col("values.tagline").alias("tagline"),
    F.col("values.title").alias("title"),
    F.col("values.video").alias("video"),
    F.col("values.vote_average").alias("vote_average"),
    F.col("values.release_date").alias("release_date"),
    F.col("values.vote_count").alias("vote_count"),
)


query = result_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "film") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

query = result_df.writeStream.outputMode("append").format("console").start()

query.awaitTermination()


# # Assuming 'transformed_df' is your DataFrame after the previous transformations
# result_df = result_df.withColumn("vote_average", col("vote_average").cast(DoubleType()))
# result_df = result_df.withColumn("vote_count", col("vote_count").cast(DoubleType()))


# query = result_df.writeStream \
#     .format("org.elasticsearch.spark.sql") \
#     .outputMode("append") \
#     .option("es.resource", "movie") \
#     .option("es.nodes", "localhost") \
#     .option("es.port", "9200") \
#     .option("es.nodes.wan.only", "false")\
#     .option("es.index.auto.create", "true")\
#     .option("checkpointLocation", "./checkpointLocation/") \
#     .start()


# query = result_df.writeStream.outputMode("append").format("console").start()

# query.awaitTermination()


# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, to_date
# from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, DoubleType, ArrayType

# spark = SparkSession.builder \
#     .appName("KafkaSparkIntegration") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
#             "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
#     .config("spark.sql.adaptive.enabled", "false") \
#     .getOrCreate()


# # Configure Kafka settings
# kafka_brokers = "localhost:9092"
# kafka_topic = "user_data_topic"

# # Define a schema for the incoming data
# schema = StructType([
#     StructField("adult", BooleanType(), True),
#     StructField("backdrop_path", StringType(), True),
#     StructField("genre_ids", ArrayType(IntegerType()), True),
#     StructField("id", IntegerType(), True),
#     StructField("original_language", StringType(), True),
#     StructField("original_title", StringType(), True),
#     StructField("overview", StringType(), True),
#     StructField("popularity", DoubleType(), True),
#     StructField("poster_path", StringType(), True),
#     StructField("release_date", StringType(), True),
#     StructField("title", StringType(), True),
#     StructField("video", BooleanType(), True),
#     StructField("vote_average", DoubleType(), True),
#     StructField("vote_count", IntegerType(), True)
# ])
# # Create a Kafka DataFrame
# df = spark.readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_brokers) \
#     .option("subscribe", kafka_topic) \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Deserialize the JSON data from Kafka
# parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# # Apply data transformations
# transformed_df = parsed_df.withColumn("release_date", to_date("release_date", "yyyy-MM-dd"))

# # Write data to Elasticsearch
# es_write_conf = {
#     "es.nodes": "localhost:9200",
#     "es.port": "9200",
#     "es.resource": "movie_index/movie_type"
# }

# try:
#     query = transformed_df.writeStream \
#         .format("org.elasticsearch.spark.sql") \
#         .outputMode("append") \
#         .option("es.resource", es_write_conf["es.resource"]) \
#         .option("es.nodes", es_write_conf["es.nodes"]) \
#         .option("es.port", es_write_conf["es.port"]) \
#         .option("es.nodes.wan.only", "true") \
#         .option("es.index.auto.create", "true") \
#         .option("checkpointLocation", "./checkpointLocation/") \
#         .start()

#     query.awaitTermination()

#     print("Successfully connected to Elasticsearch. Spark can send data.")

# except Exception as e:
#     print(f"Failed to connect to Elasticsearch. Error: {str(e)}")
# finally:
#     # Stop the Spark session
#     spark.stop()

