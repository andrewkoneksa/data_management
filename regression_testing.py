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
#  refFile:               reference file to compare against e.g. data/LPS16677_GENV3-ACTIGRAPHY_KONEKSA_202202011127_C.dat
#  newFile:               file being compared to reference e.g.  data/LPS16677_GENV3-ACTIGRAPHY_KONEKSA_202205021131_C.dat
#
##################################################################

import copy
import os
import sys
import pandas as pd
import datetime
import pytz
import numpy as np
import logging
from detect_delimiter import detect
args = sys.argv # get command line arguments, refFile and newFile
# initialize output directory and log file
outdir = 'out'
if not os.path.exists(outdir):
    os.mkdir(outdir)
dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
logFile = os.path.join(outdir, ''.join(('regression_testing_', dt, '.log')))
logging.basicConfig(filename=logFile, format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)

# Check that command line arguments are files
def readFiles(args):
    """
    This function reads in the input files, checks they exist and have similar formats
    :param args: Command line arguments containing the reference and new files
    :return: df_ref and df_new are data frames containing reference and new file contents
    """
    refFileName = args[1]
    newFileName = args[2]
    if not os.path.exists(refFileName):
        logging.error('Reference file' + refFileName + 'does not exist.')
        print(''.join(('Reference file', refFileName, 'does not exist. Exiting...')))
        exit(1)
    if not os.path.exists(newFileName):
        logging.error('New file' + newFileName + 'does not exist.')
        print('New file', newFileName, 'does not exist. Exiting...')
        exit(1)

    logging.info('Reference file  ' + refFileName)
    logging.info('New File  ' + newFileName)
    print('Reference file:\t', refFileName)
    print('New File:\t', newFileName)
    refFileName = args[1]
    newFileName = args[2]
    refExt = os.path.splitext(refFileName)[1]
    newExt = os.path.splitext(newFileName)[1]

    if refExt != newExt:
        logging.error('Reference and new file have different file types.')
        print('Reference and new file have different file types. Exiting...')
        exit(1)
    refFile = open(refFileName, "r")
    newFile = open(newFileName, "r")

    # get first line of file
    refHeader = refFile.readline()
    newHeader = newFile.readline()

    refFile.close()
    newFile.close()
    # compare headers
    if refHeader != newHeader:
        logging.error('Headers do not match.')
        print('Headers do not match. Exiting...')
        exit(1)

    # get delimiter for files
    delim = detect(refHeader)
    logging.info('Detected delimiter  ' + delim)
    print('Detected delimiter:',delim)
    # initialize data froms for reference and new files as empty lists
    df_ref=[]
    df_new=[]
    # check the extensions, read in data appropriately
    if refExt == '.dat':
        df_ref = pd.read_table(refFileName, sep=delim)
        df_new = pd.read_table(newFileName, sep=delim)
    elif refExt == '.csv':
        df_ref = pd.read_csv(refFileName, sep=delim)
        df_new = pd.read_csv(newFileName, sep=delim)
    else:
        logging.error('Unrecognized file type' + refExt)
        print('Unrecognized file type',refExt)
        exit(1)
    # return data frames containing file contents
    return df_ref, df_new

def compareFiles(df_ref,df_new):
    """
    Compare file contents, generates output files for lines that are different
    :param df_ref: Reference dataframe
    :param df_new: New dataframe
    :return:
    """
    # merge data frames
    df_all = df_ref.merge(df_new,on=df_ref.columns.values.tolist(),how='outer',indicator=True)
    refNotNew = df_all[df_all['_merge']=='left_only']# lines only in reference data fraome
    newNotRef = df_all[df_all['_merge']=='right_only']# lines only in new dataframe


    # check if file contents are the same or not
    if (refNotNew.shape[0] == 0) & (newNotRef.shape[0] == 0):
        logging.info('Files have the same contents')
        logging.info('Number of records in files  ' + str(df_ref.shape[0]))
        print('Files have the same contents')
        print('Number of records in each file:\t', df_ref.shape[0])
    else:
        logging.warning('Files do NOT have the same contents')
        print('Files do NOT have the same contents')
        # name output dataframes
        refNotNewFile = os.path.join(outdir, ''.join(('inNewNotRef_', dt, '.csv')))
        newNotRefFile = os.path.join(outdir, ''.join(('inRefNotNew_', dt, '.csv')))
        # print number of rows in each file and that are different
        logging.info('Number of records in Reference  ' + str(df_ref.shape[0]))
        logging.info('Number of records in New  ' + str(df_new.shape[0]))
        logging.info('Records in Reference not in New  ' + str(refNotNew.shape[0]))
        logging.info('Records in New not in Reference  ' + str(newNotRef.shape[0]))
        print('Number of records in Reference:  ', df_ref.shape[0])
        print('Number of records in New:  ', df_new.shape[0])
        print('Records in Reference not in New:', refNotNew.shape[0])
        print('Records in New not in Reference:', newNotRef.shape[0])
        # save file differences to output
        refNotNew.to_csv(refNotNewFile, header=True,sep=',',index=False)
        newNotRef.to_csv(newNotRefFile, header=True,sep=',', index=False)


if __name__ == "__main__":
   [df_ref, df_new] = readFiles(args) # read in data
   compareFiles(df_ref, df_new) # compare data

