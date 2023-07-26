# NAbleETLTask


######################################################################################################

#########################      University Data Load System(POC)     ##################################

######################################################################################################

Overview :
This is a poc for loading of university fixed width data file to db

Assumptions and changes to sample files:
  1. Minor change was done for studentid since both ids were same, in order to make it unque, changed one id.
  2. Fixed width file had some differences in length interms of provided data_dict file, so added couple of spaces to taly the columns lengths with values.

Tools used :
DB : Posgres
Pipeline tool : Airflow
Scripting Language : Python
Container : Docker

Orchestration :
  whole pipeline is created and orchestrated with Airflow. dag_id='daily_load_university_data'

Sequance of The Load :
1. Parse fixed width Uni data file with requested column set.
2. Check and create schema of the table or entire table according to the data_dict.txt file.
3. Load data to raw level in the DB.
4. Load data to prod level (dbo) in DB with qaulity checking.
5. move old files to archive folders.

Methods and checks for above steps :

1. Parse fixed width Uni data file with requested column set.
	a. Script will dynamically create set of columns as per needed using the parameters in data_dict.txt file and there can be only one data_dict.txt in the location.
	b. when creating the parsed file created date and time will be concatenated.
	c. Follow the comments in file for steps.

	
2. Check and create schema of the table or entire table according to the data_dict.txt file.
	a. Script will only check if there are missing columns in db table comparing to the column set requested for the current fle.
	b. And add those columns to db table .
	c. Follow comments in file for the steps.
	
3. Load data to raw level in the DB.
	a. It will load all the data available in current file to the raw table which will act as the dim table.
	b. Column set will be dynmically picked for each data_dict file and will add id and file name as columns.
	c. So it will be able to identify each row for recieved file.
	d. Follow comments in file for the steps.
	
4. Load data to prod level (dbo) in DB with qaulity checking.
	a. Once data is loaded to raw, it will extract the data and create fact tables.
	b. Also will ensure to ignore duplicate raws.

5. Move old files to archive folders.
	a. once everything is success it will move parsed data file, recieved file to archive folders
	
Python Libraries used : Pandas,Numpy,psycopg2,os,glob,shutil,datetime,airflow,logging
	
Database Design :
		1. two schemas, raw and dbo where dbo has fact tables and raw has dim table.
		2. One raw table to hold all time recieved data. Act also as the dim table.
		3. Two fact tables to store unique student details and university data.

Spinnup Docker Environment :
	1. Install docker, steps as follows https://docs.docker.com/desktop/install/windows-install/
	2. Go into the project folder locate docker compose file
	3. open cmd to the above location and run, "docker compose up -d"
