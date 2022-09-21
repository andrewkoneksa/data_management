import os
import pandas as pd
import numpy as np
from detect_delimiter import detect
import io

class fileDetails:
    """
    gets info on file
    """

    def __init__(self, qcloader):
        data_path = os.path.join(qcloader.input_dir, qcloader.config_data['File_Details']['file'])
        self.data_path = data_path
        self.data_df = qcloader.readFile(data_path)
        self.config_data = qcloader.config_data
        self.input_dir = qcloader.input_dir
        self.output_dir = qcloader.output_dir
        self.logFile = qcloader.logFile
    def getMetaData(self):
        dataFileName = self.data_path
        if not os.path.exists(dataFileName):
            print(''.join(('Data file', dataFileName, 'does not exist. Exiting...')))
            exit(1)
        dataFileName = os.path.abspath(dataFileName)
        # Get metaData
        #  File name,  file location, file extension, delimiter,num subjects, num rows,
        #  num columns, column names,num missing values per column
        print('Input file:\t', os.path.basename(dataFileName))
        print('File Location:\t', os.path.dirname(dataFileName))
        metaFile = os.path.join(self.output_dir, ''.join((os.path.basename(dataFileName).split('.')[0], '_metaData', '.csv')))
        file = open(self.data_path, "r")
        head = file.readline()
        file.close()
        delim = detect(head)
        ext = os.path.splitext(dataFileName)[1]
        input_df = pd.read_csv(dataFileName,sep=delim)

        print('Delimiter:\t', delim)
        print('Extension:\t', ext)
        numrows = input_df.shape[0]
        numcols = input_df.shape[1]
        print('Number of rows:\t', numrows)
        print('Number of columns:\t', numcols)
        print()
        cols = input_df.columns.tolist()
        cols_na = input_df.isna().sum()
        sub_id_name = self.config_data["File_Details"]["Subject_ID"]
        num_subjects = len(np.unique(input_df[sub_id_name]))
        meta_dict = {
            'Name': ['file name', 'file location', 'file extension', 'delimiter', 'rows', 'columns', 'subjects'],
            'Value': [os.path.basename(dataFileName), os.path.dirname(dataFileName), ext, delim, numrows, numcols,
                      num_subjects]}
        meta_df = pd.DataFrame(meta_dict)
        meta_df.loc[len(meta_df.index)] = ['column name', 'missing count']
        print('column name\t missing count')
        print()
        meta_df.to_csv(metaFile)

    def listSubjects(self):
        """
        Get list of subjects
        """
        dataFileName = os.path.basename(self.data_path)
        subjFile = os.path.join(self.output_dir, ''.join(((dataFileName).split('.')[0], '_subjects', '.csv')))
        sub_id_name = self.config_data['File_Details']['Subject_ID']
        subjects = pd.DataFrame(data=np.unique(self.data_df[sub_id_name]), columns=[sub_id_name])
        subjects = subjects.set_index(sub_id_name)
        subjects.to_csv(subjFile)
        print('List of subjects:')
        for s in subjects.index:
            print(s)

    def getMeasures(self):
        """
        Get list of measures.
        """
        dataFileName = os.path.basename(self.data_path)
        measFile = os.path.join(self.output_dir, ''.join((dataFileName.split('.')[0], '_measures', '.csv')))
        meas_id_name = self.config_data['File_Details']["Measurement"]
        value_id_name = self.config_data['File_Details']["Value"]
        units_id_name = self.config_data['File_Details']["Units"]
        data_df = self.data_df.astype(str)
        measures = np.unique(data_df[meas_id_name])
        meas_df = pd.DataFrame(columns=['Measure', 'Units', 'Precision'])
        for m in measures:
            sub_df = data_df[data_df[meas_id_name] == m]
            u = ','.join(np.unique(sub_df[units_id_name].dropna()))
            v = sub_df[value_id_name]
            lst = [len((s).split('.')[1]) for s in v if '.' in str(s)]
            p = ','.join(list(map(str, np.unique(lst))))
            meas_df.loc[len(meas_df.index)] = [m, u, p]
        meas_df = meas_df.set_index('Measure')
        meas_df.to_csv(measFile)
        print(meas_df)
