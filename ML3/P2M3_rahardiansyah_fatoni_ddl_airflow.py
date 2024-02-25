import pandas as pd
import psycopg2 as db

url = 'https://www.kaggle.com/datasets/gregorut/videogamesales'

conn_string = "dbname='airflow' host='localhost' user='airflow' password='airflow' port=5434"
conn = db.connect(conn_string)
cur = conn.cursor()

sql = '''
    CREATE TABLE IF NOT EXISTS table_m3 (
    "Rank" BIGINT PRIMARY KEY,
    "Name" VARCHAR(255),
    "Platform" VARCHAR(50),
    "Year" DECIMAL(10, 2),
    "Genre" VARCHAR(50),
    "Publisher" VARCHAR(100),
    "NA_Sales" DECIMAL(10, 2),
    "EU_Sales" DECIMAL(10, 2),
    "JP_Sales" DECIMAL(10, 2),
    "Other_Sales" DECIMAL(10, 2),
    "Global_Sales" DECIMAL(10, 2)
);
    '''

cur.execute(sql)
conn.commit()

df = pd.read_csv('P2M3_rahardiansyah_fatoni_data_raw.csv')
df = df.drop(columns=['Unnamed: 0'])

for index, row in df.iterrows():
    insert_query = 'INSERT INTO table_m3 ("Rank", "Name", "Platform", "Year", "Genre", "Publisher", "NA_Sales", "EU_Sales", "JP_Sales", "Other_Sales", "Global_Sales") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    values = tuple(row)
    cur.execute(insert_query, values)

conn.commit()
conn.close()
