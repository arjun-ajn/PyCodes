###############################################################################
#    GetCip        Ranjith Gopalankutty               05-02-2019              #
#    This python is snapped it up for proceeing the items which has CIP flag  #
#    turned on in IDF and ensure it has got matching Reversal or H record in  #
#    in CIPPMR file, else it will be an issue                                 #
###############################################################################

"""
VERSION 2.0 - Arjun M
Organized inputs and outputs to separate directories. Planned archiving feature for future use.

VERSION 2.1 - Arjun M
Added a archive function to write the input files as CSVs and all the files compressed to ARCHIVE.CSV for DB updates.
DB updates are planned for future

VERSION 2.2 - Arjun M
Added configuration file for input and output variables to be configured on the config file.

VERSION 2.3 - Arjun M
Added archiving function that will compress the INPUT & OUTPUT folders to Archive directory and maintain daily backups.
Configured this to prepare email body for NFM mail server as per the ADD records issue.

VERSION 3.1 - Arjun M
Added PPFI processing. Now PPFI records will be backed up as well.

VERSION 3.2 - Arjun M
This will report if the GETCIP.LOG file is not present for a store. Also took out PPFI processing as it is causing
memory issues.
Then testing all list processing directly to dataframes.

"""

import sys
import time
import datetime
import os
import glob
from pathlib import Path
import re
import pandas as pd
import csv
import numpy
import zipfile
import shutil
import linecache
import getpass
import configparser

# I dont know , why i need so many lists but let it be.

# FixedPath = "D:/DS/EPOS/GETCIP/"

StoreList = []  # List of all stores for CIP checks
ItemList = []
PMRList = []
UniqueItemList = []
StoreCountList = []
CIPOTLIST = []
ArcRecords = []
AddAnalyticsStores = []

CIPOTlist = []
CIPPMRlist = []
CIPRMlist = []
CIPBAKlist = []
CIPLOGlist = []
ADDList = []
CIPPOKlist = []
AddStores = []
PPFIlist = []
# Tday = datetime.datetime.now().strftime("%Y%m%d")
Yday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")


config = configparser.ConfigParser()
if config.read('cipConfig.ini') == 0:
    # config.read('./bdcConfigh.ini')
    FixedPath = "C:/EPOS/GETCIP/"
    InputDir = "INPUT/"
    OutputDir = "OUTPUT/"
    ResultFile = "ARCHIVE.csv"
    LogFile = "ActivityLog.txt"
    ReportFile = "ReportLog.txt"
    GETCIP = "GETCIP.txt"
    AddReport = "AddAnalysis.txt"
    ItemReport = "ItemAnalysis.txt"
    StoreReport = "StoreAnalysis.txt"
    ArchFile = "getcip.zip"
    ArchDir = "Archive/"
else:
    # config.sections()
    FixedPath = config.get('inputfiles', 'FixedPath')
    InputDir = config.get('inputfiles', 'InputDir')
    ResultFile = config.get('outputfiles', 'ResultFile')
    LogFile = config.get('outputfiles', 'LogFile')
    ReportFile = config.get('outputfiles', 'ReportFile')
    GETCIP = config.get('outputfiles', 'GETCIP')
    AddReport = config.get('outputfiles', 'AddReport')
    ItemReport = config.get('outputfiles', 'ItemReport')
    StoreReport = config.get('outputfiles', 'StoreReport')
    OutputDir = config.get('outputfiles', 'OutputDir')
    ArchFile = config.get('outputfiles', 'ArchFile')
    ArchDir = config.get('outputfiles', 'ArchDir')


# Error Handling Logs
def LogIt(typ, txt):
    if typ == "a":
        ActivityLog.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : " + txt + "\n")
        print(txt)
    elif typ == "r":
        ResultLog.write(txt + "\n")
        # print(txt)
    elif typ == "p":
        print(txt + "\n")
    elif typ == "e":
        # ActivityLog.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : " + str(txt) + "\n")
        exc_type, exc_obj, tb = sys.exc_info()
        f = tb.tb_frame
        lineno = tb.tb_lineno
        filename = f.f_code.co_filename
        linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        err = ("File : " + filename
               + "\nLine No. : " + str(lineno)
               + "\nLine : " + str(line.strip())
               + "\nReason : " + str(exc_obj)
               + "\n")
        LogIt("a", err)
        LogIt("p", err)
        os.system('pause')
        exit()
    else:
        print("INVALID LOG ERROR!")
        os.system('pause')
        exit()


# Open Files
def FileOpen(file, mode):
    # LogIt("a", "FileOpen : Opening " + file + " in " + mode + "mode")
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    # LogIt("a", "FileOpen : Opening " + file)
    file.close()


def ArchiveRecords():
    try:
        LogIt("a", "ArchiveRecords : Initializing")
        for root, dirs, files in os.walk(FixedPath + InputDir):
            LogIt("a", "ArchiveRecords : Processing files in " + FixedPath + InputDir)
            for filename in files:
                # if filename[-3:] != 'PFI':  # Ignoring PPFI files as it is causing memory issues due to it's huge size
                with open(FixedPath + InputDir + filename, "r") as f:
                    for rec in f:
                        _rec = filename[-3:] + "," + filename[:-4] + "," + rec.rstrip()
                        _rec = _rec.replace('\"', '')
                        ArcRecords.append(_rec)
                # else:
                #     pass
            LogIt("a", "ArchiveRecords : Processed all files in " + FixedPath + InputDir)
        LogIt("a", "ArchiveRecords : Processing " + str(len(ArcRecords)) + " records in ArcRecords")
        _cols = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        # the variable _cols may cause issues with below lines in this function
        dfCIPLOG = pd.DataFrame(columns=_cols)
        dfCIPPMR = pd.DataFrame(columns=_cols)
        dfCIPBAK = pd.DataFrame(columns=_cols)
        dfCIPRM = pd.DataFrame(columns=_cols)
        dfCIPOT = pd.DataFrame(columns=_cols)
        dfCIPOK = pd.DataFrame(columns=_cols)
        dfPPFI = pd.DataFrame(columns=_cols)
        for rec in ArcRecords:
                if rec.split(',')[0] == "POT":
                    if rec.split(',')[2][0] == "H":
                        pos = [1, 5, 9, 17]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "D":
                        pos = [1]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "A":
                        pos = [1, 8, 9, 17, 21, 29, 32, 40, 43, 51, 54, 62, 65, 73]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "T":
                        pos = [1, 9]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPOT = dfCIPOT.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPOTlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                elif rec.split(',')[0] == "PMR":
                    if rec.split(',')[2][0] == "H":
                        pos = [1, 8, 9, 17, 25, 28, 33, 36, 44, 47, 55, 58, 66, 69]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "A":
                        pos = [1, 8, 9, 17, 21, 29, 32, 40, 43, 51, 54, 62, 65, 73]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "R":
                        pos = [1, 8, 16, 24, 32]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "D":
                        pos = [1]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPPMR = dfCIPPMR.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPPMRlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                elif rec.split(',')[0] == "PRM":
                    if rec.split(',')[2] != '':
                        if rec.split(',')[2][6] == "A":
                            pos = [6, 7, 14, 15, 23, 31, 33, 42, 45, 78]
                            rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                        elif rec.split(',')[2][6] == "D":
                            pos = [6, 7, 14]
                            rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                        elif rec.split(',')[2][6] == "R":
                            pos = [6, 7, 14, 22, 30, 38, 46, 47]
                            rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                        elif rec.split(',')[2][6] == "H":
                            pos = [6, 7, 14, 15, 23, 31, 34, 42, 45, 53, 56, 64, 67, 75, 78]
                            rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    else:
                        rec += ',,,,,,,,,,,,,,,,,,'
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPRM = dfCIPRM.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPRMlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                elif rec.split(',')[0] == "BAK":
                    if rec.split(',')[2][0] == "A" or rec.split(',')[2][0] == "H":
                        pos = [1, 8, 9, 17, 25, 28, 36, 39, 47, 50, 58, 61, 69]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "D":
                        pos = [1]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    elif rec.split(',')[2][0] == "R":
                        pos = [1, 8, 16, 24, 32]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPBAK = dfCIPBAK.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPBAKlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                elif rec.split(',')[0] == "POK":
                    pos = [4, 8, 16, 17, 25, 31, 32, 35, 36, 37, 45, 51, 52, 60, 66, 67, 68]
                    rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPOK = dfCIPOK.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPPOKlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                elif rec.split(',')[0] == "PFI":
                    if rec.split(',')[2][0] == "9999999":
                        pos = [7, 8, 13]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    else:
                        pos = [7, 8, 14, 19, 20, 21, 29]
                        rec = rec[:14] + ','.join(rec[14:][i:j] for i, j in zip([None] + pos, pos + [None]))
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfPPFI = dfPPFI.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # PPFIlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
                else:
                    rec = datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec
                    dfCIPLOG = dfCIPLOG.append(pd.Series(rec.split(','), index=_cols[0:len(rec.split(','))]), ignore_index=True)
                    # CIPLOGlist.append(datetime.datetime.now().strftime("%d-%m-%Y") + "," + rec)
        LogIt("a", "ArchiveRecords : Processed : " + str(len(ArcRecords)) + " records.")
        LogIt("a", "CIPPMR archived today : " + str(len(CIPPMRlist)))
        LogIt("a", "CIPOT archived today : " + str(len(CIPOTlist)))
        LogIt("a", "CIPRM archived today : " + str(len(CIPRMlist)))
        LogIt("a", "CIPBAK archived today : " + str(len(CIPBAKlist)))
        LogIt("a", "CIPLOG archived today : " + str(len(CIPLOGlist)))
        LogIt("a", "Preparing DataFrames from lists:")
        # dfCIPPMR = pd.DataFrame([line.split(",") for line in CIPPMRlist])
        # dfCIPOT = pd.DataFrame([line.split(",") for line in CIPOTlist])
        # dfCIPRM = pd.DataFrame([line.split(",") for line in CIPRMlist])
        # dfCIPBAK = pd.DataFrame([line.split(",") for line in CIPBAKlist])
        # dfCIPLOG = pd.DataFrame([line.split(",") for line in CIPLOGlist])
        # dfCIPPOK = pd.DataFrame([line.split(",") for line in CIPPOKlist])
        if len(ADDList) > 0:
            dfCIPADD = pd.DataFrame([line.split(",") for line in ADDList])
            LogIt("r", "Below are the count of items in stores that failed R record creation :")
            dfCIPADD.iloc[:, 2].value_counts().to_csv(FixedPath + OutputDir + 'adds.txt')
            with open(FixedPath + OutputDir + 'adds.txt') as f:
                for line in f:
                    LogIt("r", line.rstrip())
            dfCIPLOG.append([dfCIPBAK, dfCIPRM, dfCIPOT, dfCIPPMR, dfCIPADD, dfCIPOK], ignore_index=True) \
                .to_csv(FixedPath + OutputDir + ResultFile, index=False, chunksize=10000)
        else:
            LogIt("r", "No add records failed processing today.")
            dfCIPLOG.append([dfCIPBAK, dfCIPRM, dfCIPOT, dfCIPPMR, dfCIPOK], ignore_index=True, sort='2') \
                .to_csv(FixedPath + OutputDir + ResultFile, index=False, chunksize=10000)
        LogIt("a", "ARCHIVED Everything to " + FixedPath + OutputDir + ResultFile)
        ArcRecords.clear()
        CIPLOGlist.clear()
        CIPBAKlist.clear()
        CIPRMlist.clear()
        CIPOTlist.clear()
        CIPPMRlist.clear()
        ADDList.clear()
        PPFIlist.clear()

        '''
        # COMMENTED AS NOT MANDATORY IN TEST ENVIRONMENT
        def zipdir(path, ziph):
            # ziph is zipfile handle
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file[-3:] != "zip":
                        ziph.write(os.path.join(root, file))
            LogIt("a", "ARCHIVED " + path + " to " + FixedPath + ArchFile)
        zipf = zipfile.ZipFile(FixedPath + ArchFile, 'w', zipfile.ZIP_DEFLATED)
        zipdir(FixedPath + InputDir, zipf)
        zipdir(FixedPath + OutputDir, zipf)
        zipf.close()
        shutil.copyfile(FixedPath + ArchFile,
                        FixedPath + ArchDir + datetime.datetime.now().strftime("%d-%m-%Y") + '.zip')
        LogIt("a", "ARCHIVES Copied to " + FixedPath + ArchDir + " as "
              + datetime.datetime.now().strftime("%d-%m-%Y") + ".zip")
        '''
    except Exception as e:
        LogIt("e", str(e))


def AnalyzeFurther():
    try:
        FileName = FixedPath + OutputDir + "getcip.txt"
        with open(FileName, encoding="Latin-1") as f3:
            for line in f3:
                UniqueItemList.append(line[20:27])
                StoreCountList.append(line[6:10])
            ItemCount = pd.Series(UniqueItemList).value_counts().to_dict()
            StoreCount = pd.Series(StoreCountList).value_counts().to_dict()
            LogIt("a", "Now sorting item codes against store counts")
            ItemAnalytics.writelines(
                "******************************************************************************" + "\n")
            ItemAnalytics.writelines(
                "*                     Item Level count report                                *" + "\n")
            ItemAnalytics.writelines(
                "******************************************************************************" + "\n")
            for element in ItemCount:
                ItemAnalytics.writelines("item Code " + str(element) + " Has CIP reversal missing in " + str(
                    ItemCount[element]) + " stores" + "\n")
            LogIt("a","Now checking number of items affected per store")
            StoreAnalytics.writelines(
                "******************************************************************************" + "\n")
            StoreAnalytics.writelines(
                "*                     Store Level count report                               *" + "\n")
            StoreAnalytics.writelines(
                "******************************************************************************" + "\n")
            for element in StoreCount:
                StoreAnalytics.writelines("Store " + str(element) + " Has " + str(
                    StoreCount[element]) + " items in cip without matching R record" + "\n")
    except Exception as e:
        LogIt("e", str(e))


def processcip():
    '''
    This function checks if the store's CIP active items have a corresponding CIPPMR record.
    For each store from store list, it's corresponding CIPPMR record is checked for R/H which is required for reversal.
    If an R/H is not found, it is reported to GETCIP.txt
    This also function checks if Adds are processed into CIPPMR as H/R/A
    '''
    try:
        Filepath = FixedPath + InputDir
        for i in range(len(StoreList)):
            LogIt("a", "now checking for " + StoreList[i])
            # Process the store IDF list and add to a list
            LogFileName = StoreList[i] + ".LOG"
            FullPath = Filepath + LogFileName
            with open(FullPath, encoding="Latin-1") as f1:
                for line in f1:
                    ItemList.append(line[2:9])
            # Now add the current stores PMR file in to a list
            PMRFileName = StoreList[i] + ".PMR"
            FullPath = Filepath + PMRFileName
            with open(FullPath, encoding="Latin-1") as f2:
                for line in f2:
                    PMRList.append(line[:8])
            # Now lets do the comparison we are looking for R record let me tell you
            # but we will be happy with H as well :-).

            for j in range(len(ItemList)):
                FirstTry = "R" + ItemList[j]
                try:
                    b = PMRList.index(FirstTry)
                except ValueError:
                    SecondTry = "H" + ItemList[j]
                    if SecondTry in ItemList:
                        pass
                    else:
                        file.writelines("Store " + PMRFileName[5:9] + " has item " + ItemList[
                            j] + " without matching reversal record" + "\n")
                        # ArcRecords.append(PMRFileName[:9] + ",NOR," + ItemList[j])  # ADD EACH LINE FOR ARCHIVE
                else:
                    pass
            # Need to call CIPOT processing as well only if
            # there are add request for that day else ignore that store for that day.
            # NOTE THAT THE CIPOT FILE HEADER DATE SHOULD BE CHECKED TO CONFIRM THE CIPOT DATE

            CIPOTFileName = StoreList[i] + ".POT"
            FullPath = Filepath + CIPOTFileName
            AddRecordCount = 0  # initialize it before each
            try:
                with open(FullPath, encoding="Latin-1") as f4:
                    if Yday == f4.readline()[9:17]:
                        LogIt("a", StoreList[i] + " has received a CIPOT file " + FullPath)
                        for line in f4:
                            if line[:1] == "A":
                                # Fetch the add record items to a List
                                AddRecordCount = AddRecordCount + 1
                                CIPOTLIST.append(line[:8])
                        # Now check if you need to a comparison against CIPPMR list based on the count
                        if AddRecordCount > 0:
                            for k in range(len(CIPOTLIST)):
                                if CIPOTLIST[k][-7:] in ItemList:
                                    try:
                                        Rtype = "R" + CIPOTLIST[k][-7:]
                                        b = PMRList.index(Rtype)
                                    except ValueError:
                                        Htype = "H" + CIPOTLIST[k][-7:]
                                        if Htype in PMRList:
                                            pass
                                        else:
                                            AddAnalytics.writelines(
                                                "Store " + str(PMRFileName[5:9]) + " received Add request for item "
                                                + str(CIPOTLIST[k][-7:])
                                                + " without matching reversal or held record in PMR" + "\n")
                                            AddAnalyticsStores.append(PMRFileName[5:9])
                                            ADDList.append(datetime.datetime.now().strftime("%d-%m-%Y") + ",ADD," +
                                                           str(PMRFileName[:9]) + "," + str(CIPOTLIST[k][-7:]))
                                    else:
                                        pass
                                else:
                                    LogIt("a", "Mark down hasn't applied to " + CIPOTLIST[k][-7:] +
                                          " may be due to various factors, store" + FullPath)
                                    # mark down hasn't applied may be due to various factors such as planner leaver
                                    # date hasn't met and is in pending list until then , so need to worry about
                                    # such items
                                    pass
                        LogIt("a", str(AddRecordCount) + " Add records found in " + FullPath)
                    else:
                        # This means that CIPOT file is not the latest & add processing check can be skipped
                        LogIt("a", StoreList[i] + " had outdated CIPOT file " + FullPath)
                # Now clear both list before next set of store
                PMRList.clear()
                ItemList.clear()
                CIPOTLIST.clear()
            except Exception as e:
                ErrorString = '{c} - {m}'.format(c=type(e).__name__, m=str(e)) + "\n"
                file.writelines(ErrorString)
                PMRList.clear()
                ItemList.clear()
        AddStores = list(set(AddAnalyticsStores))
        if len(AddAnalyticsStores) > 0:
            LogIt("r", "Stores which received A records, but no R records were created.")
            LogIt("r", str(set(AddAnalyticsStores)))
        AddAnalyticsStores.clear()
        file.close()
    except Exception as e:
        LogIt("e", str(e))



def Getcip():
    '''
    Get the list of stores to process from the GETCIP.LOG file names.
    If there is no corresponding CIPPMR file, we can skip the store can be skipped as CIP is not active.
    CIPPMR file not found will be reported.
    Then the A processing will be checked on stores.
    '''
    try:
        for root, dirs, files in os.walk(FixedPath + InputDir):
            for filename in files:
                if filename[-3:] == 'LOG' or filename[-3:] == 'log':
                    # now ensure a matching PMR file exist
                    PMRFile = filename[:9] + ".PMR"
                    if PMRFile in files:
                        StoreList.append(filename[:9])
                    else:
                        LogIt("a", "No CIPPMR file for store " + filename[:-3])
                        pass
                else:
                    pass
        processcip()
    except Exception as e:
        LogIt("e", str(e))


###############################################################################
#                                                                             #
#                     Start of Mainline Code                                  #
#                                                                             #
###############################################################################
start = time.time()

# Log Files
# ActivityLog = FileOpen(os.path.join(FixedPath + LogFile), "w+")
# ResultLog = FileOpen(os.path.join(FixedPath + ReportFile), "w+")
with open(os.path.join(FixedPath + OutputDir + LogFile), "w+") as ActivityLog:
    with open(os.path.join(FixedPath + OutputDir + ReportFile), "w+") as ResultLog:
        LogIt("a", "Initializing application by " + getpass.getuser())

        # Log Header
        LogIt("r", "------------------------------------------------------------------------------------------------")
        # LogIt("r", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
        LogIt("r", " Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        LogIt("r", "------------------------------------------------------------------------------------------------")

        # Open the reporting files GETCIP, AddAnalysis, ItemAnalysis & StoreAnalysis
        file = open(FixedPath + OutputDir + GETCIP, "w+")
        AddAnalytics = open(FixedPath + OutputDir + AddReport, "w+")
        Getcip()
        ItemAnalytics = open(FixedPath + OutputDir + ItemReport, "w+")
        StoreAnalytics = open(FixedPath + OutputDir + StoreReport, "w+")
        AnalyzeFurther()
        end = time.time()
        TotalTime = end - start
        ItemAnalytics.close()
        StoreAnalytics.close()
        ArchiveRecords()
        # Log Footer
        LogIt("r", "------------------------------------------------------------------------------------------------")
        LogIt("r", "Completed at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        LogIt("r", "------------------------------------------------------------------------------------------------")

        LogIt("r", "Program completed in " + str(TotalTime))
os.system('pause')
