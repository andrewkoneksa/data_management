#!/usr/bin/env python3
# Copyright 2022 Koneksa Health. All rights reserved.
#
#
# Authors: Amnah Eltahir
##################################################################
#
#  This file runs the automated qc functions functions in the qclib library.
#
#  REQUIRED ENVIRONMENT VARIABLES
#
#  REQUIRED COMMAND LINE ARGUMENTS
#
#  yamlFile:               File specifying QC
#
##################################################################

import sys
from qclib import io, compareFTS, compareVersions, consistencyCheck, fileDetails

args = sys.argv  # get config file (YAML file)

config_file = args[1]
# load data from YAML config file
qcloader = io.QCLoader(config_file)
qcloader.readYAML()

if 'IO' not in qcloader.config_data:
    print('No IO info specified. Exiting...')
    exit(1)

# Module 3
if 'Consistency_Check' in qcloader.config_data:
    cc = consistencyCheck.consistencyCheck(qcloader)
    if 'Duplicates' in qcloader.config_data['Consistency_Check']:
        cc.checkDuplicates()
    if 'Overlap' in qcloader.config_data['Consistency_Check']:
        cc.checkOverlap()
    if 'Measurement_Statistics' in qcloader.config_data['Consistency_Check']:
        cc.measurementStatistics()
    if 'CountCombinations' in qcloader.config_data['Consistency_Check']:
        cc.countCombinations()
    if 'Cross_Check' in qcloader.config_data['Consistency_Check']:
        cc.crossCheck()