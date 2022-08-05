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
import logging
args = sys.argv # get command line arguments, refFile and newFile
# initialize output directory and log file
outdir = 'data/output'
if not os.path.exists(outdir):
    os.mkdir(outdir)
dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
logFile = os.path.join(outdir, ''.join(('consistencyCheck_', dt, '.log')))
descFile = os.path.join(outdir, ''.join(('descriptiveStats_', dt, '.csv')))
overlapFile = os.path.join(outdir, ''.join(('overlap_',dt,'.csv')))
logging.basicConfig(filename=logFile, format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)

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
        logging.error('Unrecognized file type' + ext)
        exit(1)
    return df

def getData(args):
    # read data and config files
    dataFileName = args[1]
    configFileName = args[2]
    if not os.path.exists(dataFileName):
        print(''.join(('Data file', dataFileName, 'does not exist. Exiting...')))
        logging.error('Data file' + dataFileName + 'does not exist.')
        exit(1)
    if not os.path.exists(configFileName):
        print(''.join(('Config file', configFileName, 'does not exist. Exiting...')))
        logging.error('Config file' + configFileName + 'does not exist.')
        exit(1)
    dataFileName = os.path.abspath(dataFileName)
    configFileName = os.path.abspath(configFileName)

    print('Data file:\t', os.path.basename(dataFileName))
    logging.info('Data file ' + dataFileName)
    print('Config file:\t', os.path.basename(configFileName))
    logging.info('Config file ' + configFileName)

    input_df = readFile(dataFileName)
    config_df = readFile(configFileName)

    return input_df, config_df

def checkDuplicates(input_df, config_df):
    value_id_name = config_df[config_df.eq("Value").any(1)]['Field Name'].values[0]
    dups = input_df.drop(columns=[value_id_name]).duplicated(keep=False)
    duplicated_df = input_df[dups]
    n = duplicated_df.shape[0]
    if n==0:
        print('No duplicate rows detected.')
        logging.info('No duplicate rows detected.')
    else:
        print(n, 'rows are duplicates.')
        logging.error(str(n) + ' rows are duplicates.')
        print(duplicated_df)
        logging.info(duplicated_df)
    return

def checkOverlap(input_df, config_df):
    cat_id_name = config_df[config_df.eq("Category").any(1)]['Field Name'].values[0]
    sleep_id_name = config_df[config_df.eq("Sleep").any(1)]['Field Name'].values[0]
    steps_id_name = config_df[config_df.eq("Steps").any(1)]['Field Name'].values[0]
    categories = [sleep_id_name,steps_id_name]
    if (sleep_id_name=='nan') & (steps_id_name=='nan'):
        print('No Sleep or Steps categories specified.')
        logging.info('No Sleep or Steps categories specified.')

    else:
        stdate_id_name = config_df[config_df.eq("Start Date").any(1)]['Field Name'].values[0]
        sttime_id_name = config_df[config_df.eq("Start Time").any(1)]['Field Name'].values[0]
        endate_id_name = config_df[config_df.eq("End Date").any(1)]['Field Name'].values[0]
        entime_id_name = config_df[config_df.eq("End Time").any(1)]['Field Name'].values[0]
        sub_id_name = config_df[config_df.eq("Subject ID").any(1)]['Field Name'].values[0]
        meas_id_name = config_df[config_df.eq("Measurement").any(1)]['Field Name'].values[0]

        if ((stdate_id_name=='nan') | (sttime_id_name=='nan')) | \
                ((endate_id_name=='nan') | (entime_id_name=='nan')):
            print('Date and time columns not specified.')
            logging.info('Date and time columns not specified.')
        else:
            categories = [x for x in categories if x != 'nan']
            overlap_index_list = []
            input_df = input_df.reset_index()
            #input_df = input_df[input_df['ZPCLMD']=='DERIVED VALUE']
            for c in categories:
                subj_groups = input_df[input_df[cat_id_name]==c].groupby([sub_id_name,meas_id_name])
                for group in subj_groups:
                    idx = group[1]['index'].reset_index(drop=True)
                    st_df = pd.to_datetime(group[1][stdate_id_name] + ' ' + group[1][sttime_id_name])
                    en_df = pd.to_datetime(group[1][endate_id_name] + ' ' + group[1][entime_id_name])
                    for i in range(1,len(idx)):
                        delta_t=(st_df[idx[i]] - en_df[idx[i-1]]).total_seconds()
                        if delta_t<0:
                            overlap_index_list.append((input_df.loc[idx[i],sub_id_name],idx[i-1],idx[i],c,input_df.loc[idx[i],meas_id_name]))
            if len(overlap_index_list)>0:
                df_overlap = pd.DataFrame(overlap_index_list,columns=['SubjID','Previous','Next','Category','Measure'])
                print('Error: The following rows have overlapping times:')
                logging.error('Error: The following rows have overlapping times:')
                print(df_overlap)
                logging.error(df_overlap)
                df_overlap.to_csv(overlapFile)
            else:
                print('No overlapping times detected.')
                logging.info('No overlapping times detected.')


    return

def measurementStatistics(input_df, config_df):
    meas_id_name = config_df[config_df.eq("Measurement").any(1)]['Field Name'].values[0]
    value_id_name = config_df[config_df.eq("Value").any(1)]['Field Name'].values[0]
    input_df[value_id_name] = pd.to_numeric(input_df[value_id_name])
    desc_groups = input_df.groupby(meas_id_name)[value_id_name].describe()
    desc_groups.to_csv(descFile)
    logging.info('Descriptive stats saved to ' + descFile)
    print(desc_groups)


    return

if __name__ == "__main__":
    [input_df,config_df] = getData(args)
    checkDuplicates(input_df, config_df)
    checkOverlap(input_df,config_df)
    measurementStatistics(input_df,config_df)