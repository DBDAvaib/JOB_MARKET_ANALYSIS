def Test_Engineer_queries(Test_Engineer_stream):
    """
    Function to handle queries specific to Data Analyst job postings.
    This function can be extended to include more complex queries or aggregations.
    """
    # Example query: Count the number of job postings per skill
    Query1 = Test_Engineer_stream.groupBy("sort_title").count()
    
    # Output the counts to console for debugging
    Query1.writeStream \
        .outputMode("complete") \
        .trigger(processingTime='10 seconds') \
        .format("parquet") \
        .option("path", "./Big_Data/Query_Result") \
        .option("checkpointLocation", checkpoint_path) \
        .start()
    
    Query1.awaitTermination()