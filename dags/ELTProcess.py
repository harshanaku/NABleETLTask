import pandas as pd
import glob
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import execute_values
import logging

#arrays needed
columnlist = []

#pick latest column set for the file
list_of_files = glob.glob('/opt/airflow/dags/data/Dictionaries/RepeatedColumns/repeated_columns_for_this_file_*')
latest_file = max(list_of_files, key=os.path.getctime)

#create column set identical to db table
with open(latest_file, 'r') as fp:
    for item in fp:
        item = item.strip().lower()
        item = item.replace(' ','_')
        columnlist.append(item)

columnlist.insert(0,'id')
columnlist.insert(len(columnlist),'filename')

#check connection to postgres
try:
    conn = psycopg2.connect(database="UniversityDB",
                        user='airflow', password='airflow',
                        host='postgres', port='5432'
                        )
except:
    logging.warning("Connection failed to Postgres....")

else:
    logging.warning("Connection established to Postgres successfully!")

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
pgcursor = conn.cursor()

#reading through parsed fwf file and loading to dataframe
df = pd.concat(
  [
    pd.read_csv(filename).assign(source=filename)
    for filename in glob.glob("/opt/airflow/dags/data/ParsedFiles/*.csv")
  ],
  ignore_index=True
)

#converting dataframe to a list so that it can directly load to DB
list = df.values.tolist()

#inserting whole list to the Db in one go inluding id and filenames for each data file
execute_values(pgcursor, "INSERT INTO raw.dim_university_data ("+','.join(columnlist)+") VALUES %s", list)

#commiting execution
conn.commit();
