import os
import pandas as pd
import numpy as np


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
