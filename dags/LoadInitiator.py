from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

#Airflow Dag default settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

#initiating Dag
with DAG(
    dag_id='daily_load_university_data',
    default_args=default_args,
    start_date=datetime(2023, 7, 20)
#defining Dag
) as dag:
    Parse_Fixed_Width_File = BashOperator(
        task_id='parse_fwf',
        bash_command="python3 /opt/airflow/dags/parsefwf.py",
    )

    Check_Schema_According_to_File = BashOperator(
    task_id='check_schema',
    bash_command="python3 /opt/airflow/dags/CreateSchemaForRecievedFile.py",
    )

    Load_Data_to_Raw_Table = BashOperator(
    task_id='loading_raw',
    bash_command="python3 /opt/airflow/dags/ELTProcess.py",
    )

    Load_Data_to_Dbo_Table = BashOperator(
    task_id='loading_dbo',
    bash_command="python3 /opt/airflow/dags/LoadDbo.py",
    )

    Move_Old_Files_Once_Complete = BashOperator(
    task_id='moving_old_files',
    bash_command="python3 /opt/airflow/dags/MoveOldFilestoArchive.py",
    )

    #sequance to follow
    Parse_Fixed_Width_File >> Check_Schema_According_to_File >> Load_Data_to_Raw_Table >> Load_Data_to_Dbo_Table >> Move_Old_Files_Once_Complete