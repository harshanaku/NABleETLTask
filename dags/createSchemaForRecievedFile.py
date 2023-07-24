import glob
import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

#arrays as needed
columninsertlist = []
columnlist = []
tablecolumnnames = []
new_list = []

#pick latest file with full columns set for the current data file
list_of_files = glob.glob('/opt/airflow/dags/data/Dictionaries/RepeatedColumns/all_columns_for_this_file_*')
latest_file = max(list_of_files, key=os.path.getctime)

#create column set identical to the table in db
with open(latest_file, 'r') as fp:
    for item in fp:
        item = item.strip().lower()
        item = item.replace(' ','_')
        columninsertlist.append('"'+item+'"'+" "+'VARCHAR')
        columnlist.append(item)

columnlist.insert(0,'id')
columnlist.insert(-1,'filename')

#check the connection to postgres
try:
    conn = psycopg2.connect(database="UniversityDB",
                        user='airflow', password='airflow',
                        host='postgres', port='5432'
                        )
except:
    logging.warning("Error : Connection failed to Postgres....")

else:
    logging.warning("Connection established to Postgres successfully!")

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
pgcursor = conn.cursor()

#check if table exists if not create else get the existsing column set in db
try:
    sql = ("select * from raw.dim_university_data")
    pgcursor.execute(sql)

except:
    sql = "CREATE TABLE IF NOT EXISTS raw.dim_university_data (ID INT," + ",".join(columninsertlist) + ")"
    pgcursor.execute(sql)

else:
    tablecolumnnames = [desc[0] for desc in pgcursor.description]

#check if there are missing column from db
if len(tablecolumnnames) == 0:
    for i in columnlist:
        if i.lower() not in tablecolumnnames:
            new_list.append(i)

    for i in tablecolumnnames:
        if i.lower() not in columnlist:
            new_list.append(i)

#check add missing columns in db table
if len(tablecolumnnames) > 0 or len(new_list) > 0:
    for i in new_list:
        sql = ("ALTER TABLE raw.dim_university_data ADD COLUMN IF NOT EXISTS "+ i + " VARCHAR")
        pgcursor.execute(sql)

