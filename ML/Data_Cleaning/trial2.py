import json
import re
import pandas as pd

# Read JSON file and convert it into a Python list
with open("./java_data.json", "r") as file:
    data1 = json.load(file)

with open("./Data_Analyst_data.json", "r") as file:
    data2 = json.load(file)

# with open("./Data_Engineer_data.json", "r") as file:
#     data3 = json.load(file)

with open("./Test_Engineer_data.json", "r") as file:
    data4 = json.load(file)

with open("./Software_Engineering_data.json", "r") as file:
    data5 = json.load(file)

# with open("./DevOps_Data.json", "r") as file:
#     data6 = json.load(file)

# Initialize lists
title = []
java = []
min_exp = []
max_exp = []
spring_boot = []
sql = []
git = []
microservices = []
hibernate = []
min_salary = []
max_salary = []
Power_BI = []
Excel = []
Python = []
Visualization = []
spark = []
AWS = []
Cloud_Computing = []
automation_testing = []
regression_testing = []
software_testing = []
manual_testing = []
Kubernetes = []
Docker = []
Jenkins = []
CI_CD = []
title2 = []

datas = [data1,data2,data4,data5]
I=0
for data in datas :
    for str_content in data:
        java.append(1 if "Java" in str_content or "java" in str_content else 0)
    
        # Extract title
        title.append(str_content.split("\n")[0])

        if(I==0):
            title2.append("Java")
        if(I==1):
            title2.append("Data_Analyst")
        if(I==2):
            title2.append("Test_Engineer")
        if(I==3):
            title2.append("Software_Engineer")

        # Extract experience values
        match_exp = re.search(r'(\d+)-?(\d*)\s*Yrs', str_content)
    
        if match_exp:
            min_exp.append(match_exp.group(1))  # First number before "Yrs"
            max_exp.append(match_exp.group(2) if match_exp.group(2) else match_exp.group(1))  # Handle optional second number
        else:
            min_exp.append(None)
            max_exp.append(None)

        # Extract salary values
        match_salary = re.search(r'(\d+\.?\d*)-?(\d*\.?\d*)?\s*Lacs PA', str_content)

        if match_salary:
            min_salary.append(match_salary.group(1))  # First salary before "Lacs PA"
            max_salary.append(match_salary.group(2) if match_salary.group(2) else match_salary.group(1))  # Handle optional second number
        else:
            min_salary.append(None)
            max_salary.append(None)

        # Keyword-based feature extraction
        spring_boot.append(1 if "spring" in str_content or "Spring" in str_content else 0)
        sql.append(1 if "sql" in str_content or "Sql" in str_content else 0)
        git.append(1 if "git" in str_content or "Git" in str_content else 0)
        microservices.append(1 if "microservices" in str_content or "Microservices" in str_content else 0)
        hibernate.append(1 if "hibernate" in str_content or "Hibernate" in str_content else 0)
        Power_BI.append(1 if "Bi" in str_content or "BI" in str_content else 0)
        Excel.append(1 if "Excel" in str_content or "excel" in str_content else 0)
        Python.append(1 if "Python" in str_content or "python" in str_content else 0)
        Visualization.append(1 if "Visualization" in str_content or "visualization" in str_content else 0)
        spark.append(1 if "spark" in str_content or "Spark" in str_content or "hive" in str_content or "Hive" in str_content else 0)
        Cloud_Computing.append(1 if "Cloud" in str_content or "cloud" in str_content or "Computing" in str_content or "computing" in str_content else 0)
        manual_testing.append(1 if "Manual" in str_content or "manual" in str_content else 0)
        regression_testing.append(1 if "Regression" in str_content or "regression" in str_content else 0)
        automation_testing.append(1 if "Automation" in str_content or "automation" in str_content else 0)
        software_testing.append(1 if "Software" in str_content or "software" in str_content else 0)
        Kubernetes.append(1 if "Kubernetes" in str_content or "kubernetes" in str_content else 0)
        Docker.append(1 if "Docker" in str_content or "docker" in str_content else 0)
        Jenkins.append(1 if "Jenkins" in str_content or "jenkins" in str_content else 0)
        CI_CD.append(1 if "CI" in str_content or "ci" in str_content else 0)
    I+=1

# Create DataFrame
data_dict = {
    "Title": title,
    "Short_Title" : title2,
    "Java": java,
    "Min Experience": min_exp,
    "Max Experience": max_exp,
    "Min Salary": min_salary,
    "Max Salary": max_salary,
    "Spring Boot": spring_boot,
    "SQL": sql,
    "Git": git,
    "Microservices": microservices,
    "Hibernate": hibernate,
    "Power_BI" : Power_BI,
    "Excel" : Excel,
    "Python" : Python,
    "Visualization" : Visualization,
    "spark" : spark,
    "Cloud_Computing" : Cloud_Computing,
    "manual_testing" : manual_testing,
    "regression_testing" : regression_testing,
    "automation_testing" : automation_testing,
    "software_testing" : software_testing,
    "Kubernetes" : Kubernetes,
    "Docker" : Docker,
    "Jenkins" : Jenkins,
    "CI/CD" : CI_CD
}

df = pd.DataFrame(data_dict)

print(df)  # Print to verify consistency

# Save to CSV
df.to_csv("Final_Combined_data.csv", index=False)