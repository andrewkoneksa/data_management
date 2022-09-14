import os
import pandas as pd
import logging
from detect_delimiter import detect
import yaml
from yaml.loader import SafeLoader
import datetime


class QCLoader:
    """
    loads in configurations and data for QC

    """

    def __init__(self, yamlFile):
        self.yamlFile = yamlFile

    def readYAML(self):
        if not os.path.exists(self.yamlFile):
            print(''.join(('Config file', self.yamlFile, 'does not exist. Exiting...')))
            exit(1)
        yamlFileName = os.path.abspath(self.yamlFile)
        print('Config file:\t', os.path.basename(yamlFileName))
        with open(yamlFileName, "r") as stream:
            try:
                config_data = yaml.load(stream, Loader=SafeLoader)
            except yaml.YAMLError as exc:
                print(exc)
        self.config_data = config_data
        self.input_dir = config_data['IO']['input_dir']
        self.output_dir = config_data['IO']['output_dir']
        dt = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        logFile = os.path.join(self.output_dir, ''.join((os.path.splitext(os.path.basename(yamlFileName))[0],
                                                         '_',dt,'.log')))
        self.logFile = logFile
        logging.basicConfig(filename=logFile, format='%(asctime)s:%(levelname)s:%(message)s', level=logging.DEBUG)
        logging.info('Config file ' + yamlFileName)

    def readFile(self, fileName):
        file = open(os.path.join(self.input_dir,fileName), "r")
        head = file.readline()
        file.close()
        delim = detect(head)
        ext = os.path.splitext(fileName)[1]

        df = []
        if ext == '.dat':
            df = pd.read_table(fileName, sep=delim)
        elif ext == '.csv':
            df = pd.read_csv(fileName, sep=delim)
        else:
            print('Unrecognized file type', ext)
            logging.error('Unrecognized file type' + ext)
            exit(1)
        return df

    def getData(self,dataFileName):
        # read data file
        if not os.path.exists(os.path.join(self.input_dir,dataFileName)):
            print(''.join(('Data file', dataFileName, 'does not exist. Exiting...')))
            logging.error('Data file' + dataFileName + 'does not exist.')
            exit(1)
        print('Data file:\t', dataFileName)
        logging.info('Data file ' + dataFileName)

        dataFileName = os.path.abspath(os.path.join(self.input_dir,dataFileName))



        input_df = self.readFile(dataFileName)


        return input_df


