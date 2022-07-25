#!/usr/bin/env python3
# Copyright 2022 Koneksa Health. All rights reserved.
#
#
# Authors: Amnah Eltahir

##################################################################
#
#  REQUIRED ENVIRONMENT VARIABLES
#
#  REQUIRED COMMAND LINE ARGUMENTS
#
#  dataFile:               file to be inspected
#  configFile:             config file describing columns in dataFile
#
##################################################################

import copy
import os
import sys
import pandas as pd
import datetime
import pytz
import numpy as np
from detect_delimiter import detect
args = sys.argv # get command line arguments, refFile and newFile
# initialize output directory and log file
outdir = 'out'
if not os.path.exists(outdir):
    os.mkdir(outdir)

#### Get Meta Data
def getMetaData(args):
    dataFileName = args[1]
    configFileName = args[2]
    if not os.path.exists(dataFileName):
        print(''.join(('Reference file', dataFileName, 'does not exist. Exiting...')))
        exit(1)
    if not os.path.exists(configFileName):
        print(''.join(('Config file', configFileName, 'does not exist. Exiting...')))
        exit(1)
    dataFileName = os.path.abspath(dataFileName)
    configFileName = os.path.abspath(configFileName)
    # Get metaData
    #  File name,  file location, file extension, delimiter,num subjects, num rows,
    #  num columns, column names,num missing values per column
    print('Input file:\t', os.path.basename(dataFileName))
    print('File Location:\t', os.path.dirname(dataFileName))
    metaFile = os.path.join(outdir, ''.join((os.path.basename(dataFileName).split('.')[0],'_metaData_','.csv')))

    [input_df,delim, ext] = readFile(dataFileName)
    print('Delimiter:\t',delim)
    print('Extension:\t',ext)
    numrows = input_df.shape[0]
    numcols = input_df.shape[1]
    print('Number of rows:\t',numrows)
    print('Number of columns:\t',numcols)
    print()
    cols = input_df.columns.tolist()
    cols_na = input_df.isna().sum()

    [config_df,c_delim,c_ext] = readFile(configFileName)
    sub_id_name = config_df[config_df.eq("Subject ID").any(1)]['Field Name'].values[0]
    num_subjects = len(np.unique(input_df[sub_id_name]))


    meta_dict = {'Name':['file name','file location','file extension', 'delimiter','rows','columns','subjects'],
                 'Value':[os.path.basename(dataFileName),os.path.dirname(dataFileName), ext, delim,numrows,numcols,num_subjects]}
    meta_df=pd.DataFrame(meta_dict)
    meta_df.loc[len(meta_df.index)] = ['column name','missing count']
    print('column name\t mising count')
    for i, cols_na_i in enumerate(cols_na):
        print(cols[i],'\t',cols_na_i)
        meta_df.loc[len(meta_df.index)] = [cols[i],cols_na_i]
    print()
    meta_df = meta_df.set_index('Name')
    meta_df.to_csv(metaFile)
    return input_df, config_df

#### read file
def readFile(fileName):
    file = open(fileName, "r")
    head = file.readline()
    file.close()
    delim = detect(head)
    ext = os.path.splitext(fileName)[1]

    df = []
    if ext == '.dat':
        df = pd.read_table(fileName, sep=delim, dtype=str)
    elif ext == '.csv':
        df = pd.read_csv(fileName, sep=delim, dtype=str)
    else:
        print('Unrecognized file type', ext)
        exit(1)
    return [df, delim, ext]
#### Get list of subjects
def listSubjects(args,input_df,config_df):
    dataFileName = args[1]
    subjFile = os.path.join(outdir, ''.join((os.path.basename(dataFileName).split('.')[0],'_subjects_','.csv')))
    sub_id_name = config_df[config_df.eq("Subject ID").any(1)]['Field Name'].values[0]
    subjects = pd.DataFrame(data=np.unique(input_df[sub_id_name]),columns=[sub_id_name])
    subjects = subjects.set_index(sub_id_name)
    subjects.to_csv(subjFile)
    print('List of subjects:')
    for s in subjects.index:
        print(s)
    print()

#### Get measures
# measures, units, precision
def getMeasures(args, input_df,config_df):
    dataFileName = args[1]
    measFile = os.path.join(outdir, ''.join((os.path.basename(dataFileName).split('.')[0],'_measures_','.csv')))
    meas_id_name = config_df[config_df.eq("Measurement").any(1)]['Field Name'].values[0]
    value_id_name = config_df[config_df.eq("Value").any(1)]['Field Name'].values[0]
    units_id_name = config_df[config_df.eq("Units").any(1)]['Field Name'].values[0]

    measures = np.unique(input_df[meas_id_name])
    meas_df = pd.DataFrame(columns=['Measure','Units','Precision'])
    for m in measures:
        sub_df = input_df[input_df[meas_id_name]==m]
        u = ','.join(np.unique(sub_df[units_id_name].dropna()))
        v = sub_df[value_id_name]
        lst=[]
        lst = [len((s).split('.')[1]) for s in v if '.' in str(s)]
        p = ','.join(list(map(str,np.unique(lst))))
        meas_df.loc[len(meas_df.index)] = [m,u,p]
    meas_df = meas_df.set_index('Measure')
    meas_df.to_csv(measFile)
    print(meas_df)
    print()

if __name__ == "__main__":
   [input_df,config_df] = getMetaData(args) # read in data
   listSubjects(args,input_df,config_df)
   getMeasures(args,input_df,config_df)

