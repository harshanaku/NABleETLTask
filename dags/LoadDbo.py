import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

#if the connection fails to postgres it will retry but a warning will logging in
try:
    conn = psycopg2.connect(database="UniversityDB",
                        user='airflow', password='airflow',
                        host='postgres', port='5432'
                        )

except:
        logging.warning("Connection failed to Postgres....")

else:
    logging.warning("Connection established to Postgres succussfully!....")

conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
pgcursor = conn.cursor()

#insert all data to dbo students table
sql = ("INSERT INTO dbo.fact_students_data (studentid, fullname, dob, country, university)\
                         SELECT DISTINCT student_id,full_name,to_date(cast(date_of_birth as TEXT),'YYYY-MM-DD'),country, university\
                         FROM raw.dim_university_data \
                         GROUP BY university, student_id,full_name,date_of_birth,country \
                         ORDER BY student_id")
pgcursor.execute(sql)

#remove duplicate entries in students table where all columns are considrered
sql2 = ("DELETE FROM dbo.fact_students_data\
            WHERE studentid IN \
                    (SELECT studentid \
                        FROM \
                        (SELECT studentid,\
                            ROW_NUMBER() OVER(PARTITION BY university, studentid,fullname,dob,country ) AS row_num \
                                FROM dbo.fact_students_data) t\
        WHERE t.row_num > 1 )")
pgcursor.execute(sql2)

#insert all unique data to dbo university data table
sql = ("INSERT INTO dbo.dim_university_data \
                         SELECT DISTINCT *\
                         FROM raw.dim_university_data\
                         WHERE NOT EXISTS (SELECT * FROM dbo.dim_university_data)\
                         ORDER BY student_id")
pgcursor.execute(sql)

conn.commit()