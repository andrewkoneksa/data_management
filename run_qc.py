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

# Module 1
if 'Compare_Versions' in qcloader.config_data:
    cv = compareVersions.compareVersions(qcloader)
    cv.compareFiles()
# Module 2
if 'File_Details' in qcloader.config_data:
    fd = fileDetails.fileDetails(qcloader)
    fd.listSubjects()
    fd.getMeasures()

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

# Module 4
if 'Compare_FTS' in qcloader.config_data:
    cfts = compareFTS.compareFTS(qcloader)
    if 'Column Names' in qcloader.config_data['Compare_FTS']:
        cfts.checkColumns()
    if 'Level List' in qcloader.config_data['Compare_FTS']:
        cfts.checkMeasures()
