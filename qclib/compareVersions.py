import os
import logging


class compareVersions:
    """
    compares file contents to check for differences

    """

    def __init__(self, qcloader):
        ref_path = os.path.join(qcloader.input_dir, qcloader.config_data['Compare_Versions']['refFile'])
        new_path = os.path.join(qcloader.input_dir, qcloader.config_data['Compare_Versions']['newFile'])
        self.ref_file = ref_path
        self.new_file = new_path
        self.ref_df = qcloader.readFile(ref_path)
        self.new_df = qcloader.readFile(new_path)
        self.input_dir = qcloader.input_dir
        self.output_dir = qcloader.output_dir
        self.logFile = qcloader.logFile

    def compareFiles(self):
        """
        Compare file contents, generates output files for lines that are different
        """
        # merge data frames
        df_ref = self.ref_df
        df_new = self.new_df
        df_all = df_ref.merge(df_new, on=df_ref.columns.values.tolist(), how='outer', indicator=True)
        refNotNew = df_all[df_all['_merge'] == 'left_only']  # lines only in reference data fraome
        newNotRef = df_all[df_all['_merge'] == 'right_only']  # lines only in new dataframe
        logging.basicConfig(filename=self.logFile, format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
        logging.info('Reference File: ' + os.path.basename(self.ref_file))
        logging.info('New File: ' + os.path.basename(self.new_file))
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
            fname = os.path.splitext(os.path.basename(self.new_file))[0]
            refNotNewFile = os.path.join(self.output_dir, ''.join((fname, '_inNewNotRef.csv')))
            newNotRefFile = os.path.join(self.output_dir, ''.join((fname, '_inRefNotNew.csv')))
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
            refNotNew.to_csv(refNotNewFile, header=True, sep=',', index=False)
            newNotRef.to_csv(newNotRefFile, header=True, sep=',', index=False)
