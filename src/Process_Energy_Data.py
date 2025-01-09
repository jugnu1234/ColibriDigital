import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# Define the schema for raw data, include nulls as well
schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("turbine_id", IntegerType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("power_output", DoubleType(), True)
])

# Bronze table - Raw data ingestion
@dlt.table(
    name="bronze_data",
    comment="Raw wind turbine data from CSV files",
    schema=schema
)
def get_bronze_data():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.allowOverwrites", "true") #files are overwritten with new data
            .option("header", "true")
            .schema(schema)
            .load("abfss://test@<ADLSgen2 account>.dfs.core.windows.net/dlt/data") #this can be parametarised to point to a different folder
    )

# Silver table - Cleaned data
@dlt.table(
    name="silver_data",
    comment="Cleaned wind turbine data with missing values handled and outliers removed"
)
@dlt.expect_or_drop("valid_power_output", "power_output >= 0 AND power_output <= 10")
@dlt.expect_or_drop("valid_wind_speed", "wind_speed >= 0 AND wind_speed <= 100")
@dlt.expect_or_drop("valid_wind_direction", "wind_direction >= 0 AND wind_direction < 360")
def get_silver_data():
    return (
       dlt.read("bronze_data")
            # Remove rows with null values
            .dropna(subset=["timestamp", "turbine_id", "power_output"])
            # Convert timestamp to proper format
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            # Add window column for 24-hour grouping
            .withColumn(
                "Loadedtime",
                F.current_timestamp()
            )
    )

# Gold table - 24-hour summary statistics
@dlt.table(
    name="gold_daily_stats",
    comment="Daily summary statistics for each turbine"
)
def get_daily_stats():
    return (
        dlt.read("silver_data")
            .groupBy(  "turbine_id",
            F.window(F.col("timestamp"), "24 hours").alias("window")) # Add window column for 24-hour grouping
            .agg(
                F.avg("power_output").alias("avg_power"),
                F.min("power_output").alias("min_power"),
                F.max("power_output").alias("max_power"),
                F.stddev("power_output").alias("std_power"),
                F.count("power_output").alias("reading_count")
            )
            # Calculate anomaly bounds
            .withColumn("upper_bound", F.col("avg_power") + (2 * F.col("std_power")))
            .withColumn("lower_bound", F.col("avg_power") - (2 * F.col("std_power")))
            # Extract window start and end for better readability
            .withColumn("window_start", F.col("window.start"))
            .withColumn("window_end", F.col("window.end"))
            .drop("window")
    )

# Create a view of anomalies joining it with the silver table
@dlt.table(
    name="gold_anomalies",
    comment="Individual readings enriched with window statistics"
)
def get_anomalies():
    silver_data = dlt.read("silver_data")
    daily_stats = dlt.read("gold_daily_stats")
    
    return (
        silver_data
            .join(
                daily_stats,
                (silver_data.turbine_id == daily_stats.turbine_id) &
                (silver_data.timestamp >= daily_stats.window_start) &
                (silver_data.timestamp < daily_stats.window_end),
                "inner"
            )
            .withColumn(
                "is_anomaly",
                (F.col("power_output") > F.col("upper_bound")) |
                (F.col("power_output") < F.col("lower_bound"))
            ).drop(silver_data.turbine_id).filter("is_anomaly = true").dropDuplicates()
    )
