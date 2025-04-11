"""
Creating an ETL pipeline using Pandas, SQLite and python
Will be using CSV files and/or API's to source the data public data
- Extract the data from the source's
- Transform and clean the extracted data
- Load the transformed data into a relational database

Once these steps are complete, I would like to automate the processes with Apache airflow or Prefect

We will be pulling data from a kaggle dataset that encompasses global cyber security threats between 2015 and 2024

"""

import pandas as pd
import csv
import sqlite3

def extract_data(source):
    print("Extracting data...")
    return pd.read_csv(source)

# Lets transform the data, we may clean, omit or add data where required

# Here we are selecting the relevent columns 
def transform_data(df):
    print("Transforming data...")
    df = df[['Country', 'Year', 'Attack_Type', 'Target_Industry', ' s', 'Attack_Source', 'Security_Vulnerability_Type', 'Defense_Mechanism_Used', 'Incident_Resolution_Time_Hours']]

# Handle any missing values in the following line
    df = df.dropna()

# This dataset is fairly clean already, so no adjustments really need to do at this point in time
    return df

# Lets store the data in a SQLite database

# Creating the database connection
def load_data(df, db_name):
    print ("Loading cleaned data into database... ")
    con = sqlite3.connect(db_name)

# Loading the data in the database
    df.to_sql('Cyber_threat_cases', con , if_exists ='replace', index = False)

def etl_together():
    source = 'Global_Cybersecurity_Threats_2015-2024.csv'
    db_name = 'Cyber_Threats_data.db'

    data = extract_data(source)
    transformed_data = transform_data(data)
    load_data(transformed_data, db_name)
    print("\nETL pipeline finished\n")

etl_together()