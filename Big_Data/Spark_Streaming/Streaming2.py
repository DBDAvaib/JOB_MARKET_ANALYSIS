from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, split, lit, when, lower # <-- Import 'lower' function here
from pyspark.sql.types import StringType

spark = SparkSession.builder\
            .appName('Streaming')\
            .getOrCreate()

# define source
topic_name = "Data_Analyst"
Data_Analyst_raw = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic_name) \
    .option("failOnDataLoss", "false") \
    .load()

# Processing (unchanged)
print("Start of Schema (Raw Kafka Data)")
Data_Analyst_raw.printSchema()
print("End of Schema (Raw Kafka Data)")

# Define skills and their keywords (unchanged)
skills_keywords = {
    "Spring_boot": ["spring"],
    "sql": ["sql"],
    "git": ["git"],
    "Microservices": ["microservices"],
    "Hibernate": ["hibernate"],
    "Power_BI": ["bi"],
    "Excel": ["excel"],
    "Python": ["python"],
    "Visualization": ["visualization"],
    "Spark": ["spark", "hive"],
    "Cloud_Computing" : ["cloud", "aws", "azure", "gcp"],
    "Automation_testing": ["automation testing", "selenium", "cucumber"],
    "Manual_testing": ["manual testing"],
    "Regression_testing": ["regression testing"],
    "Docker": ["docker"],
    "Kubernetes": ["kubernetes"],
    "Jenkins": ["jenkins"],
    "CI_CD": ["ci", "cd", "continuous integration", "continuous deployment"]
}

# --- STEP 1: Process raw value to get json_string, job_posting (now lowercase), Title, and sort_title ---
Data_Analyst_processed = Data_Analyst_raw.withColumn(
    "json_string",
    col("value").cast(StringType())
).withColumn(
    "job_posting", # Compute job_posting here and make it lowercase immediately
    lower(get_json_object(col("json_string"), "$")) # <--- MODIFIED HERE: Used lower() as a function
).withColumn(
    "Title",
    split(col("job_posting"), "\n").getItem(0) # 'job_posting' is available (and lowercase)
).withColumn(
    "sort_title",
    lit("Data_analyst")
)

# --- STEP 2: Dynamically add skill columns ---
# Since 'job_posting' is already lowercase, we can simplify the checks
computed_skill_columns = {}
for skill_col_name, keywords in skills_keywords.items():
    condition = None
    for keyword in keywords:
        # Now, job_posting is already lowercase, so just check for the lowercase keyword
        # .contains() is generally more efficient for simple substring checks than rlike.
        current_condition = col("job_posting").contains(keyword.lower())

        # Alternative using rlike if .contains() also gives issues:
        # current_condition = col("job_posting").rlike(f"{keyword.lower()}")

        if condition is None:
            condition = current_condition
        else:
            condition = condition | current_condition

    computed_skill_columns[skill_col_name] = when(condition, 1).otherwise(0)

# Apply all dynamically created skill columns to the DataFrame
df_final_with_all_cols = Data_Analyst_processed.withColumns(computed_skill_columns)

# --- STEP 3: Select desired columns for the final output ---
Data_Analyst_final_output = df_final_with_all_cols.select(
    "Title",
    "sort_title",
    *[col_name for col_name in skills_keywords.keys()]
)

# Start the streaming query
query1 = Data_Analyst_final_output.writeStream\
    .trigger(processingTime='10 seconds')\
    .outputMode("append") \
    .format("console")\
    .option("truncate", "false")\
    .start()

query1.awaitTermination()

spark.stop()