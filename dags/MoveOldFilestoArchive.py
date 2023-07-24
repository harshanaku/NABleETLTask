import os
import shutil

#source which needs to be cleared after all loading
source = '/opt/airflow/dags/data/ParsedFiles'
source_recieved = '/opt/airflow/dags/data/RecievedData'

#archiva location where old files will available
destination = '/opt/airflow/dags/data/archive data/'
destination_recieved = '/opt/airflow/dags/data/RecievedData/Archived'

#get all files available for parsed file loction
allfiles_parsed = os.listdir(source)

#get alls files available for recieved file location
allfiles_recieved = []

for path in os.listdir(source_recieved):
    # check if current path is a file
    if os.path.isfile(os.path.join(source_recieved, path)):
        allfiles_recieved.append(path)

#moving parsed files using for loop
for f in allfiles_parsed:
    src_path = os.path.join(source, f)
    dst_path = os.path.join(destination, f)
    shutil.move(src_path, dst_path)

#moving recieved files using for loop
for f in allfiles_recieved:
    src_path_recieved = os.path.join(source_recieved, f)
    dst_path_recieved = os.path.join(destination_recieved, f)
    shutil.move(src_path_recieved, dst_path_recieved)