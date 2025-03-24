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
from sqlalchemy import create_engine

# Reading in the information from a CSV and into a dataframe

# with open('Global_Cybersecurity_Threats_2015-2024.csv', newline='') as csvfile:
#     spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#     for row in spamreader:
#         print(', '.join(row))

source = 'Global_Cybersecurity_Threats_2015-2024.csv'

df = pd.read_csv(source)

# preview the data 
# print(df.head())

# Lets transform the data, we may clean, omit or add data where required

# Here we are selecting the relevent columns 
df_filtered = df[['Country', 'Year', 'Attack Type', 'Target Industry', 'Financial Loss (in Million $)', 'Attack Source', 'Security Vulnerability Type', 'Defense Mechanism Used', 'Incident Resolution Time (in Hours)']]

# Handle any missing values in the following line
df_filtered = df_filtered.dropna()

# This dataset is fairly clean already, so no adjustments really need to do at this point in time

# Lets store the data in a SQLite database

# Creating the database connection
engine = create_engine('sqlite:///cyber_threats_data.db')

# Loading the data in the database
df_filtered.to_sql('Cyber_threat_cases', con = engine, if_exists ='replace', index = False)

print("\nCyber threat data loaded into database successfully\n")