from datetime import datetime
import pandas as pd
import numpy

#lists and numpy arrays as needed
list = []
np = []
repeat_columns = []
repeats = []
column_set_modified = []
column_set_app = []
columns_widths = []
repeated_columns_for_this_file = []
idx = []

#variables as needed
columnid = 0
iter = 0
flagv = 0
e = 0

#read data dictionary
df = pd.read_csv("/opt/airflow/dags/data/Dictionaries/data_dict.csv")
column_names = df['Field'].values

end = df[df.columns[4]]
start = df[df.columns[3]]

#create array with requested column names
for i in column_names:
        np_array = (end[e] - start[e]) + 1
        list = numpy.append(list,np_array)
        e = e+1

#create the full column with requested column names
for i in column_names:
    NoofCols = 1
    np=[]
    column_set_app.append(i)
    if ('repeats of fields' in i):
        frequency = int(i[0])
        ilen = i.find('fields') + len('fields')
        ilen2 = i.find('(')
        i1 = i[ilen:ilen2]
        i1 = i1.split('-')
        i1 = [eval(i) for i in i1]
        start = i1[0]
        end = i1[1]
        np.extend(range(start,end))
        np.append(end)
        repeat_columns = []
        repeat_widths = []
        for l in np:
            l = l - 1
            flagv = 0
            repeat_columns.append(column_names[l])
            repeat_widths.append(list[l+iter])
            iter_cols = iter
            tempList = repeat_columns
            widthlist = repeat_widths
            repeats = []
            repeatsw = []
            flagv = 0
        for j in range(0,frequency):
            for element in tempList:
                repeats.append(element+'_'+str(flagv))

            flagv += 1
            for wv in widthlist:
                repeatsw.append(wv)
                iter += 1

        idx = numpy.append(idx,np[-1]+iter)
        list = numpy.insert(list,np[-1]+iter_cols,repeatsw)
        column_set_app.append(repeats)

#remove unneeded column ids
for n in idx:
    if (n != idx[-1]):
        list = numpy.delete(list,int(n))
    else:
        n -= 1
        list = numpy.delete(list, int(n))

#removed unneeded column names
for i in column_set_app:
    if ('repeats of fields' not in i):
        column_set_modified = numpy.append(column_set_modified, i)

#convert list to int
list = list.astype(int)
list = list.tolist()

#get current date and time
currentDateTime = datetime.now().strftime("%m-%d-%Y %H-%M-%S %p")

#create file with full column set for schema creation
with open(f'/opt/airflow/dags/data/Dictionaries/RepeatedColumns/all_columns_for_this_file_{currentDateTime}.csv', 'w') as fp:
    for item in column_set_modified:
        fp.write(item+'\n')

#parse fwf file with columns and create a data frame and then write that dataframe to csv file
df_fwf = pd.read_fwf(("/opt/airflow/dags/data/RecievedData/university_data.txt"), widths=list, names=column_set_modified)
df_fwf.to_csv(f"/opt/airflow/dags/data/ParsedFiles/university_data_parsed_{currentDateTime}.csv")

