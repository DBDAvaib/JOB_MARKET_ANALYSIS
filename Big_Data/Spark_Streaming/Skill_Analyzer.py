from pyspark.sql.functions import col, when

SKILLS_KEYWORDS = {
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
    "Cloud_Computing": ["cloud", "aws", "azure", "gcp"],
    "Automation_testing": ["automation testing", "selenium", "cucumber"],
    "Manual_testing": ["manual testing"],
    "Regression_testing": ["regression testing"],
    "Docker": ["docker"],
    "Kubernetes": ["kubernetes"],
    "Jenkins": ["jenkins"],
    "CI_CD": ["ci", "cd", "continuous integration", "continuous deployment"]
}

def add_skill_columns(df, job_posting_col_name):
    """
    Dynamically adds skill columns to a PySpark DataFrame based on keywords.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame containing a job posting column.
        job_posting_col_name (str): The name of the column with the job posting text.
                                     This column is expected to be in lowercase.

    Returns:
        pyspark.sql.DataFrame: The DataFrame with new skill columns added.
    """
    computed_skill_columns = {}
    for skill_col_name, keywords in SKILLS_KEYWORDS.items():
        condition = None
        for keyword in keywords:
            current_condition = col(job_posting_col_name).contains(keyword.lower())
            if condition is None:
                condition = current_condition
            else:
                condition = condition | current_condition
        
        computed_skill_columns[skill_col_name] = when(condition, 1).otherwise(0)
    
    return df.withColumns(computed_skill_columns)