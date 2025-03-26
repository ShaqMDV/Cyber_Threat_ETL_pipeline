import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

# using matplotlib, going to query the data that was extracted from the CSV file and create useful graphs

# First - Connect to the SQLite database

conn = sqlite3.connect('Cyber_Threats_data.db')
cursor = conn.cursor()

# Second - Query the data

query = '''
    SELECT Country, Attack_Type
    FROM Cyber_threat_cases
'''

# Load query outcome into the dataframe
df = pd.read_sql_query(query, conn)

# Plotting the result
plt.figure(figsize=(8, 5))
plt.bar(df['Country'], df['Attack_Type'], color='purple')
plt.title('Test graph, Country vs Attack types')
plt.xlabel('Country')
plt.ylabel('Attack_Type')
plt.ticks(rotation=45)
plt.tight_layout()
plt.show()

conn.close()