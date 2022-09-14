import os
import pandas as pd
import logging


class compareFTS:
    """
    compare data to specs in FTS
    """
    def __init__(self, qcloader):
        data_path = os.path.join(qcloader.input_dir, qcloader.config_data['Consistency_Check']['file'])
        self.data_path = data_path
        self.data_df = qcloader.readFile(data_path)
        self.config_data = qcloader.config_data
        self.input_dir = qcloader.input_dir
        self.output_dir = qcloader.output_dir
        self.logFile = qcloader.logFile

    def checkColumns(self):
        """
        Check column names match specified list
        """
        data_df = self.data_df.astype(str)
        cols = self.config_data['Compare_FTS']['Column Names']
        in_config_not_data = list(set(cols) - set(data_df.columns))
        in_data_not_config = list(set(data_df.columns) - set(cols))
        if (len(in_data_not_config) == 0) & (len(in_config_not_data) == 0):
            print('All columns are present.')
            logging.info('All columns are present.')
        elif (len(in_data_not_config) > 0):
            print('Unexpected column(s): ', in_data_not_config)
            logging.error('Unexpected column(s): ' + str(in_data_not_config))
        else:
            print('Missing column(s): ', in_config_not_data)
            logging.error('Missing column(s): ' + str(in_config_not_data))

    def checkMeasures(self):
        """
        Generate table of combined column specifications for measures
        """
        data_df = self.data_df.astype(str)
        level_list = self.config_data['Compare_FTS']['Level List']
        group_df = data_df.groupby(level_list)
        dict_keys_list = group_df.groups.keys()
        keys_list = []
        for d in dict_keys_list:
            keys_list.append(d)
        df = pd.DataFrame(columns=level_list, data=keys_list)

        measurement_df = df.set_index(level_list[0:-2])
        measurementFile = os.path.join(self.output_dir, ''.join((os.path.splitext(
            os.path.basename(self.data_path))[0], '_measurement_specs.csv')))
        measurement_df.to_csv(measurementFile)
        print('Measurement specifications: ', measurementFile)
        logging.error('Measurement specifications: ' + measurementFile)

    def checkSubjectStatus(self):
        data_df = self.data_df.astype(str)

