from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder\
            .appName('demo05')\
            .getOrCreate()

# define source
topic_name = "New_Job_Postings"
New_Job_Postings = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic_name) \
    .option("failOnDataLoss", "false") \
    .load()

New_Job_Postings.printSchema()

# process
read_schema = "Title STRING, Short_Title STRING, Location STRING, Java INT, Min_Experience INT, Max_Experience INT, Min_Salary INT, Max_Salary INT, Spring_Boot INT, SQL INT, Git INT, Microservices INT, Hibernate INT, Power_BI INT, Excel INT, Python INT, Visualization INT, Spark INT, Cloud_Computing INT, Manual_Testing INT, Regression_Testing INT, Automation_Testing INT, Software_Testing INT, Kubernetes INT, Docker INT, Jenkins INT, CI_CD INT"
result = New_Job_Postings\
    .selectExpr("CAST(value AS STRING) val")\
    .select(from_json("val", read_schema).alias("v"))\


result.printSchema()

# define sink
query1 = result.writeStream\
    .trigger(processingTime='10 seconds')\
    .outputMode("complete")\
    .format("console")\
    .option("truncate", "false")\
    .start()

# define sink = kafka
# query2 = result2.writeStream\
#     .trigger(processingTime='10 seconds')\
#     .outputMode("append")\
#     .format("kafka")\
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "desdavgiot") \
#     .option("checkpointLocation", "file:///tmp/ckpt")\
#     .start()

query1.awaitTermination()

spark.stop()