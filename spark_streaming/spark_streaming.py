from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, FloatType
import time
import os
from pyspark.sql.functions import unix_timestamp, col, lit


if __name__ == "__main__":


    appName = "SparkStreamingApp"
    #spark = SparkSession.builder.appName(appName).master("spark://spark-master:7077").getOrCreate()
    spark = SparkSession.builder \
        .appName(appName)\
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.connection.port", "9042") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark_args = os.getenv('STREAMING_ARGS').split()
    lane = spark_args[0]

    fcd_topic = "fcd_topic"
    emission_topic = "emission_topic"

    print("------------------------Spark Streaming----------------------------------")
    print("------fcd-------")

    fcd_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", fcd_topic) \
        .load()

    fcd_schema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_slope", FloatType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    fcd_df_parsed = fcd_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), fcd_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestep_time", (col("timestep_time") + unix_timestamp(lit("1970-01-01 00:00:00"))).cast("timestamp"))

    
    fcd_grouped_data = fcd_df_parsed.withWatermark("timestep_time", "15 minutes") \
        .groupBy(window("timestep_time", "10 minutes").alias("date"),
                                        col("vehicle_lane")) \
        .agg(approx_count_distinct("vehicle_id").alias("vehicle_count"))
    
    flattened_df = fcd_grouped_data.select(
        col("vehicle_lane"),
        col("date.start").alias("start"),
        col("date.end").alias("end"),
        col("vehicle_count")
    )

    print("------emission-------")

    emission_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", emission_topic) \
        .load()
    
    emission_schema = StructType([
        StructField("timestep_time", FloatType()),
        StructField("vehicle_CO", FloatType()),
        StructField("vehicle_CO2", FloatType()),
        StructField("vehicle_HC", FloatType()),
        StructField("vehicle_NOx", FloatType()),
        StructField("vehicle_PMx", FloatType()),
        StructField("vehicle_angle", FloatType()),
        StructField("vehicle_eclass", StringType()),
        StructField("vehicle_electricity", FloatType()),
        StructField("vehicle_fuel", FloatType()),
        StructField("vehicle_id", StringType()),
        StructField("vehicle_lane", StringType()),
        StructField("vehicle_noise", FloatType()),
        StructField("vehicle_pos", FloatType()),
        StructField("vehicle_route", StringType()),
        StructField("vehicle_speed", FloatType()),
        StructField("vehicle_type", StringType()),
        StructField("vehicle_waiting", FloatType()),
        StructField("vehicle_x", FloatType()),
        StructField("vehicle_y", FloatType())
    ])

    emission_df_parsed = emission_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), emission_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestep_time", (col("timestep_time") + unix_timestamp(lit("1970-01-01 00:00:00"))).cast("timestamp"))


    emission_df_filtered = emission_df_parsed.filter(col("vehicle_lane") == lane)

    emission_df_grouped = emission_df_parsed \
        .withWatermark("timestep_time", "2 minutes") \
        .groupBy(
        window(col("timestep_time"), "5 minutes")
        ).agg(
            min("vehicle_CO").alias("min_co"),
            max("vehicle_CO").alias("max_co"),
            avg("vehicle_CO").alias("avg_co"),
            min("vehicle_CO2").alias("min_co2"),
            max("vehicle_CO2").alias("max_co2"),
            avg("vehicle_CO2").alias("avg_co2"),
            min("vehicle_HC").alias("min_hc"),
            max("vehicle_HC").alias("max_hc"),
            avg("vehicle_HC").alias("avg_hc"),
            min("vehicle_NOx").alias("min_nox"),
            max("vehicle_NOx").alias("max_nox"),
            avg("vehicle_NOx").alias("avg_nox"),
            min("vehicle_PMx").alias("min_pmx"),
            max("vehicle_PMx").alias("max_pmx"),
            avg("vehicle_PMx").alias("avg_pmx")
        )

    # query = emission_df_grouped.writeStream \
    # .outputMode("update") \
    # .format("console") \
    # .option("truncate", False) \
    # .start()

    flattened_emission_df = emission_df_grouped \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")


    #----postavljanje podataka na cassandru

    cluster = Cluster(['cassandra-db'], port=9042)
    time.sleep(10)
    session = cluster.connect()
      
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS bigdata2
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    session.set_keyspace('bigdata2')

    session.execute("DROP TABLE IF EXISTS lane_emission_data")
    
    session.execute("""
        CREATE TABLE IF NOT EXISTS lane_emission_data (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        min_co float,
        max_co float,
        avg_co float,
        min_co2 float,
        max_co2 float,
        avg_co2 float,
        min_hc float,
        max_hc float,
        avg_hc float,
        min_nox float,
        max_nox float,
        avg_nox float,
        min_pmx float,
        max_pmx float,
        avg_pmx float,
        PRIMARY KEY (window_start)
    );
    """)

    flattened_emission_df.printSchema()

    flattened_emission_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint_emission") \
        .option("keyspace", "bigdata2") \
        .option("table", "lane_emission_data") \
        .outputMode("append") \
        .start() \
        .awaitTermination()

    session.execute("DROP TABLE IF EXISTS vehicle_data")

    session.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_data (
        vehicle_lane TEXT,
        start TIMESTAMP,
        end TIMESTAMP,
        vehicle_count BIGINT,
        PRIMARY KEY (vehicle_lane, start)
    );
    """)


    flattened_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("checkpointLocation", "/tmp/checkpoint_fcd") \
        .option("keyspace", "bigdata2") \
        .option("table", "vehicle_data") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
    
    

    
    print("Podaci su uspešno ubačeni u Cassandru.")

    spark.streams.awaitAnyTermination()


