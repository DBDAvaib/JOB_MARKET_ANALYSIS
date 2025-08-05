from pyspark.sql import SparkSession

# --- Configuration (Add these at the top of your script or pass as arguments) ---
MYSQL_JDBC_DRIVER = "mysql-connector-java-8.0.28.jar"  # Path to your MySQL JDBC driver JAR
MYSQL_URL = "jdbc:mysql://localhost:3306/Data_Analyst"  
MYSQL_TABLE = "data_analyst_skill_counts" 
MYSQL_USER = "root"
MYSQL_PASSWORD = "Yoga@00007"

# Make sure your SparkSession is configured to include the JDBC driver
# Example (if you initialize SparkSession in the same file):
# spark = SparkSession.builder \
#     .appName("YourStreamingApp") \
#     .config("spark.jars", MYSQL_JDBC_DRIVER) \
#     .getOrCreate()
# spark.sparkContext.setLogLevel("WARN")


def Data_Analyst_queries(data_analyst_stream):
    """
    Function to handle queries specific to Data Analyst job postings.
    This function can be extended to include more complex queries or aggregations.
    """

    # --- Start the streaming queries for each stream ---
    query1 = data_analyst_stream.writeStream\
        .trigger(processingTime='10 seconds')\
        .outputMode("append") \
        .format("console")\
        .option("truncate", "false")\
        .start()
    # Example query: Count the number of job postings per skill
    Query1 = data_analyst_stream.groupBy("sort_title").count()

    # --- Function to write each batch to MySQL ---
    # This function will be called for each micro-batch of the streaming query result
    def write_to_mysql_batch(batch_df, batch_id):
        print(f"Writing batch {batch_id} (Data Analyst Skill Counts) to MySQL...")
        batch_df.write \
        .format("jdbc") \
        .option("url", MYSQL_URL) \
        .option("dbtable", MYSQL_TABLE) \
        .option("user", MYSQL_USER) \
        .option("password", MYSQL_PASSWORD) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("append") \
        .save()
        print(f"Finished writing batch {batch_id} (Data Analyst Skill Counts).")

    # Output the counts to MySQL
    Query1 = Query1.writeStream \
    .outputMode("complete") \
    .trigger(processingTime='10 seconds')\
    .foreachBatch(write_to_mysql_batch)\
    .option("checkpointLocation", "./Big_Data/Query_Result/Checkpoint/Query1_MySQL/") \
    .start()


    Query1.awaitTermination()