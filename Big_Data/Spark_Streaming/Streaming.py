from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, split, lit, lower
from pyspark.sql.types import StringType
from Skill_Analyzer import add_skill_columns, SKILLS_KEYWORDS 
from Data_Analyst_queries import *
from Data_Analyst_queries import Data_Analyst_queries
from Test_Engineer_queries import Test_Engineer_queries
from DevOps_queries import DevOps_queries
from Software_Engineering_queries import Software_Engineering_queries
from Data_Engineer_queries import Data_Engineer_queries

spark = SparkSession.builder\
            .appName('Streaming')\
            .getOrCreate()

# --- Reusable function to set up and process a stream ---
def process_stream(topic_name, sort_title,job_post ):
    # 1. Define source
    raw_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic_name) \
        .option("failOnDataLoss", "false") \
        .load()

    # 2. Process raw value to get json_string, job_posting (lowercase), Title, and sort_title
    processed_df = raw_df.withColumn(
        "json_string",
        col("value").cast(StringType())
    ).withColumn(
        job_post,
        lower(get_json_object(col("json_string"), "$"))
    ).withColumn(
        "Title",
        split(col(job_post), "\n").getItem(0)
    ).withColumn(
        "sort_title",
        lit(sort_title)
    )

    # 3. Dynamically add skill columns using the imported function
    df_with_skills = add_skill_columns(processed_df,job_post)
    
    # 4. Select desired columns for the final output
    final_output_df = df_with_skills.select(
        "Title",
        "sort_title",
        *[col_name for col_name in SKILLS_KEYWORDS.keys()]
    )
    
    return final_output_df

# --- Define and process different streams using the function ---
data_analyst_stream = process_stream(topic_name="Data_Analyst", sort_title="Data_analyst", job_post="job_posting")
#Data_Engineer_stream = process_stream(topic_name="Data_Engineer",sort_title="Data_Engineer", job_post="job_posting_Data_Engineer")
#DevOps_stream = process_stream(topic_name="DevOps",sort_title="DevOps", job_post="job_posting_DevOps")
#Software_Engineering_stream = process_stream(topic_name="Software_Engineering",sort_title="Software_Engineering", job_post="job_posting_Software_Engineering")
#Test_Engineer_stream = process_stream(topic_name="Test_Engineer",sort_title="Test_Engineer", job_post="job_posting_Test_Engineer")

Data_Analyst_queries(data_analyst_stream)
#Test_Engineer_queries(Test_Engineer_stream)
#DevOps_queries(DevOps_stream)
#Software_Engineering_queries(Software_Engineering_stream)
#Data_Engineer_queries(Data_Engineer_stream)



spark.stop()