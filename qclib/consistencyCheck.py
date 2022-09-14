import os
import pandas as pd
import numpy as np
import logging
from detect_delimiter import detect


class consistencyCheck:
    """
    check consistency and timing of data
    """

    def __init__(self, qcloader):
        data_path = os.path.join(qcloader.input_dir, qcloader.config_data['Consistency_Check']['file'])
        self.data_path = data_path
        self.data_name = os.path.basename(data_path)
        self.data_df = qcloader.readFile(data_path)
        self.config_data = qcloader.config_data
        self.input_dir = qcloader.input_dir
        self.output_dir = qcloader.output_dir
        self.logFile = qcloader.logFile

    def checkDuplicates(self):
        """
        Check for duplicates based on unique keys
        """
        unique_keys = self.config_data['Consistency_Check']['Duplicates']["Unique_keys"]
        data_df = self.data_df.astype(str)
        dups = data_df[unique_keys].duplicated(keep=False)
        duplicated_df = data_df[dups]
        n = duplicated_df.shape[0]
        if n == 0:
            print('No duplicate rows detected in ',self.config_data['Consistency_Check']['file'])
            logging.info('No duplicate rows detected in' + self.config_data['Consistency_Check']['file'])
        else:
            print(n, 'rows are duplicates in', self.config_data['Consistency_Check']['file'])
            logging.error(str(n) + ' rows are duplicates in ' + self.config_data['Consistency_Check']['file'])
            print(duplicated_df)
            duplicated_file = os.path.join(self.output_dir,
                                           ''.join((self.data_name.split('.', 1)[0], '_duplicates.csv')))
            duplicated_df.to_csv(duplicated_file)
            logging.info('Duplicates saved to ' + duplicated_file)

    def checkOverlap(self):
        """
        Check for overlapping times between rows.
        Specifically, look for overlapping times in Sleep and Steps categories
        """
        cat_id_name = self.config_data['Consistency_Check']['Overlap']["Category"]
        sleep_id_name = self.config_data['Consistency_Check']['Overlap']["Sleep"]
        steps_id_name = self.config_data['Consistency_Check']['Overlap']["Steps"]
        categories = [sleep_id_name, steps_id_name]
        if (sleep_id_name == 'nan') & (steps_id_name == 'nan'):
            print('No Sleep or Steps categories specified for', self.config_data['Consistency_Check']['file'])
            logging.info('No Sleep or Steps categories specified for ' + self.config_data['Consistency_Check']['file'])

        else:
            stdate_id_name = self.config_data['Consistency_Check']['Overlap']["Start_Date"]
            sttime_id_name = self.config_data['Consistency_Check']['Overlap']["Start_Time"]
            endate_id_name = self.config_data['Consistency_Check']['Overlap']["End_Date"]
            entime_id_name = self.config_data['Consistency_Check']['Overlap']["End_Time"]
            sub_id_name = self.config_data['Consistency_Check']['Overlap']["Subject_ID"]
            meas_id_name = self.config_data['Consistency_Check']['Overlap']["Measurement"]
            data_df = self.data_df.astype(str)
            if ((stdate_id_name == 'nan') | (sttime_id_name == 'nan')) | \
                    ((endate_id_name == 'nan') | (entime_id_name == 'nan')):
                print('Date and time columns not specified for', self.config_data['Consistency_Check']['file'])
                logging.info('Date and time columns not specified for ' + self.config_data['Consistency_Check']['file'])
            else:
                categories = [x for x in categories if x != 'nan']
                overlap_index_list = []
                data_df = data_df.reset_index()
                for c in categories:
                    subj_groups = data_df[data_df[cat_id_name] == c].groupby([sub_id_name, meas_id_name])
                    for group in subj_groups:
                        idx = group[1]['index'].reset_index(drop=True)
                        st_df = pd.to_datetime(group[1][stdate_id_name] + ' ' + group[1][sttime_id_name])
                        en_df = pd.to_datetime(group[1][endate_id_name] + ' ' + group[1][entime_id_name])
                        for i in range(1, len(idx)):
                            delta_t = (st_df[idx[i]] - en_df[idx[i - 1]]).total_seconds()
                            if delta_t < 0:
                                overlap_index_list.append((data_df.loc[idx[i], sub_id_name], idx[i - 1], idx[i], c,
                                                           data_df.loc[idx[i], meas_id_name]))
                if len(overlap_index_list) > 0:
                    df_overlap = pd.DataFrame(overlap_index_list,
                                              columns=['SubjID', 'Previous', 'Next', 'Category', 'Measure'])
                    print('Error: The following rows have overlapping times:')
                    print(df_overlap)
                    overlapFile = os.path.join(self.output_dir, ''.join((os.path.splitext(
                        os.path.basename(self.data_path))[0], '_overlap.csv')))

                    df_overlap.to_csv(overlapFile, index=False)
                    logging.error('Overlapping times saved to ' + overlapFile)
                else:
                    print('No overlapping times detected for',self.config_data['Consistency_Check']['file'])
                    logging.info('No overlapping times detected for ' + self.config_data['Consistency_Check']['file'])

    def measurementStatistics(self):
        """
        Compute 5 summary statistics for numeric data
        """
        data_df = self.data_df
        meas_id_name = self.config_data['Consistency_Check']['Measurement_Statistics']["Measurement"]
        value_id_name = self.config_data['Consistency_Check']['Measurement_Statistics']["Value"]
        self.data_df[value_id_name] = pd.to_numeric(data_df[value_id_name])
        desc_groups = data_df.groupby(meas_id_name)[value_id_name].describe()
        descFile = os.path.join(self.output_dir, ''.join((os.path.splitext(
            os.path.basename(self.data_path))[0], '_desc_stats.csv')))
        desc_groups.to_csv(descFile)
        logging.info('Descriptive stats saved to ' + descFile)
        print(desc_groups)

    def countCombinations(self):
        """
        Count unique combinations of specified column values
        """
        data_df = self.data_df
        cols = self.config_data['Consistency_Check']['Count_Combinations']["Columns"]
        combinations_df = data_df.groupby(cols).size().reset_index().rename(columns={0: 'count'})
        combinationsFile = os.path.join(self.output_dir, ''.join((os.path.splitext(
            os.path.basename(self.data_path))[0], '_combinations_count.csv')))
        combinations_df.to_csv(combinationsFile)
        logging.info('Unique combinations saved to ' + combinationsFile)
        print(combinations_df)

    def crossCheck(self):
        """
        Check if other input files contain the same values for specified columns
        :return:
        """
        data_df = self.data_df
        cross_file = self.config_data['Consistency_Check']['Cross_Check']["cross_file"]
        cross_file_path = os.path.join(self.input_dir, cross_file)
        file = open(cross_file_path, "r")
        head = file.readline()
        file.close()
        delim = detect(head)
        cross_df = pd.read_csv(cross_file_path, sep=delim)
        checkCols = self.config_data['Consistency_Check']['Cross_Check']["Columns"]
        for col in checkCols:
            data_df_vals = np.unique(data_df[col])
            cross_df_vals = np.unique(cross_df[col])
            indata = [value for value in data_df_vals if value not in cross_df_vals]
            incross = [value for value in cross_df_vals if value not in data_df_vals]
            if len(indata) > len(incross):
                msg = self.data_name + ' has more values for ' + col + ' than ' + cross_file
                print(msg)
                logging.error(msg)

            elif len(incross) > len(indata):
                msg = cross_file + ' has more values for ' + col + ' than ' + self.data_name
                print(msg)
                logging.error(msg)
            else:
                msg = 'Both ' + self.data_name + ' and ' + cross_file + ' have the same values for ' + col
                print(msg)
                logging.info(msg)
