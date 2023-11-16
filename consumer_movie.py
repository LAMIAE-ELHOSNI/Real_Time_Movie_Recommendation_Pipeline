from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType,BooleanType, IntegerType, ArrayType,DateType

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

schema =StructType([
    StructField("adult", BooleanType(), True),
    StructField("backdrop_path", StringType(), True),
    StructField("genre_ids", ArrayType(IntegerType()), True),
    StructField("id", IntegerType(), True),
    StructField("original_language", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("poster_path", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("title", StringType(), True),
    StructField("video", BooleanType(), True),
    StructField("vote_average", IntegerType(), True),
    StructField("vote_count", IntegerType(), True)
])


selected_df = value_df.withColumn("values", F.from_json(value_df["value"], schema)).selectExpr("values")
selected_df.printSchema()

result_df = selected_df.select(
    F.col("values.id").alias("id"),
    F.col("values.overview").alias("overview"),
    F.col("values.original_language").alias("original_language"),
    F.col("values.title").alias("title"),
    F.col("values.poster_path").alias("poster"),
    F.col("values.popularity").alias("popularity"),
    F.col("values.vote_average").alias("vote_average"),
    F.col("values.release_date").alias("release_date"),
    F.col("values.vote_count").alias("vote_count"),
)


query = result_df.writeStream \
    .format("org.elasticsearch.spark.sql") \
    .outputMode("append") \
    .option("es.resource", "moviesdatabase") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.nodes.wan.only", "true")\
    .option("es.index.auto.create", "true")\
    .option("checkpointLocation", "./checkpointLocation/") \
    .start()

query = result_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()