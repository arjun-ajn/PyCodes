"""

Program     :
Developer   :   Arjun Muraleedharan
Date        :   20-07-2019

Description:
This program checks for till reboots on a store by analysing the ADVAUDIT file. It gives us the tills and the number of
reboots first, then for each till having more than one reboot, all the transaction of the till are written to a single
file with till number as file name.

Functions:
1. Check the ADVAUDIT file to get the reboot counts of the tills
2. For each till, if the reboot count is more than 2, get all transactions for the till
3. Write the transaction of each issue till into a single file into a common folder "Txn"
4. Clear the existing files in the Txn folder so as to get the latest run data correctly.
5. Write all the reboots into one single file ResultLog.csv, ReportLog.csv will have a summary the entire run.

Prerequisites:
1. Create the directory path "C:/EPOS/ADV/".
2. Place the ADVAUDIT files in this directory
3. advconfig.ini in the same directory.


Initial Version V1.0
The initial version looks up the input file and creates the boot_count file which will have the till boot counts.

Version V2.0
The script is now capable of pulling all records of tills having more than one reboot. The till numbers are used as file
names and is written to a folder.

Version V3.0
This will now process one file, check for all issue tills and write all transaction for each issue till into Txns folder
The file name will contain the store, till and date

Version V3.4
Amended the code to process all files in the folder containing 'ADVAUDIT.'. All the reboot details will be written to a
single csv file ResultLog.CSV. On each run, the application will first clear all the existing files in the Txn folder so
that we get details of the latest runs.

Version V4.0
The code will now look up all the failures in the Responses. Also added a configuration file and hardcoded the existing
file paths to an if condition in case of config file missing.

"""

# Libraries
import datetime
import getpass
import os
import linecache
import sys
import pandas as pd
import configparser
import shutil
import numpy as np

# Initializing variables
RunTime = datetime.datetime.now()
config = configparser.ConfigParser()
if config.read('advconfig.ini') == 0:
    config.read('advconfig.ini')
    FixedPath = "C:/EPOS/ADV/"
    FixedName = "ADV.TXT"
    ResultFile = "ResultLog.csv"
    LogFile = "ActivityLog.txt"
    ReportFile = "ReportLog.txt"
else:
    # config.sections()
    FixedPath = config.get('inputfiles', 'FixedPath')
    FixedName = config.get('inputfiles', 'FixedName')
    ResultFile = config.get('outputfiles', 'ResultFile')
    LogFile = config.get('outputfiles', 'LogFile')
    ReportFile = config.get('outputfiles', 'ReportFile')


'''
FixedPath = "C:/EPOS/ADV/"
FixedName = "ADV.TXT"
ResultFile = "ResultLog.csv"
LogFile = "ActivityLog.txt"
ReportFile = "ReportLog.txt"
CodeFile = "CodesSuccess.txt"
'''
data = []
Rep = ''


# Error Handling Logs
def LogIt(typ, txt):
    if typ == "a":
        ActivityLog.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : " + txt + "\n")
        # print(txt)
    elif typ == "r":
        ResultLog.write(txt + "\n")
        # print(txt)
    elif typ == "R":
        ReportLog.write(txt + "\n")
    elif typ == "p":
        print(txt + "\n")
    elif typ == "e":
        # ActivityLog.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : " + str(txt) + "\n")
        print(txt)
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
        LogIt("a", "Program failed at " + str(RunTime))
        os.system('pause')
        exit()
    else:
        print("INVALID LOG ERROR!")
        LogIt("a", "Program failed at " + str(RunTime))
        os.system('pause')
        exit()


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


# Delete existing transactions
def ClearTxns():
    try:
        # Clear Transactions in the Txns folder
        LogIt("a", "ClearTxns : Initializing")
        record_count = 0
        for root, dirs, files in os.walk(FixedPath + 'Txns'):
            LogIt("a", "ClearTxns : Clearing files in " + root)
            for f in files:
                os.unlink(os.path.join(root, f))
                record_count += 1
        LogIt("a", "ClearTxns : Cleared " + str(record_count) + " files")
        LogIt("R", "Cleared Previous Tills : " + str(record_count))
        LogIt("a", "ClearTxns : Completed.")
    except Exception as e:
        LogIt("e", "GetData Error : " + str(e))


def ProcFiles(d):
    try:
        global Rep
        LogIt("a", "ProcFiles : Initializing")
        record_count = 0
        LogIt("a", "ProcFiles : Looking up ADVAUDIT files in " + d)
        for root, dirs, files in os.walk(d):
            for f in files:
                if f.find("ADVAUDIT.") == 0:
                    LogIt("a", "ProcFiles : Processing " + root + f)
                    Rep = f
                    GetData(d + f)
                    record_count += 1
        LogIt("a", "ProcFiles : Processed " + str(record_count) + " files.")
        LogIt("a", "ProcFiles : Completed")
    except Exception as e:
        LogIt("e", "ProcFiles Error : " + str(e))


# Read each file
def GetData(file):
    global Rep
    # Fetch all records from the FixedName
    LogIt("a", "GetData : Extracting " + file)
    try:
        cols = '"DATE","TIME","DEVICE","TILLNO","ESP","REQRSP","MISC","MISC1","MISC2","MISC3","MISC4","MISC5"'
        data.append(cols)
        # Open and get Details from FixedName
        with open(file) as f:
            record_count = 0
            for record in f:
                if record.find("Till") != -1 and record.find("Send:1") == -1 and record.find("Request") == -1\
                        and record.find("Resp1") == -1 and record.find("UTxn") == -1:
                    data.append(record.rstrip())
                    record_count += 1
        Rep = Rep + ", " + str(record_count)
        LogIt("a", "GetData : Completed")
        RepFails(data)
    except Exception as e:
        LogIt("e", "GetData Error : " + str(e))


# Report Fails New
def RepFails(data):
    try:
        global Rep
        LogIt("p", "RepFails : Checking failures in " + Rep[:13])
        _storeNo = ''
        _failVal = []
        # rec = ''
        for code in config.items('successcodes'):
            _record_count = 0
            _fail_count = 0
            _successVal = code[1]
            code = code[0].split(".")
            code[0] = "Response :" + code[0] + ":"
            LogIt("a", "RepFails : Checking REQ/RSP " + code[0] + " flag " + code[1])
            LogIt("p", "RepFails : Checking REQ/RSP " + code[0] + " flag " + code[1])
            for record in data:
                # print(code, record) # For Debug
                # rec = record
                _record_count += 1
                if 0 < record.find(code[0]) < 52 and record.find("Till") != -1 \
                        and record.find("Send:1") == -1 and record.find("Resp1") == -1 and record.find("UTxn") == -1:
                    record = record[record.find(code[0]):]
                    record = record.replace('"', '').split(":")
                    _storeNo = record[2][:4]
                    if int(record[int(code[1])].replace(',', '')) != int(_successVal) \
                            and len(record[int(code[1])].replace(',', '')) <= 4:
                        LogIt("a", str(record))
                        _fail_count += 1
                        LogIt("a", "Record type" + code[0] + " flag " + code[1] + " : " + record[0] + "has failure : "
                              + record[int(code[1])].replace(",", "") + " at record " + str(_record_count))
                        _failVal.append(record[int(code[1])].replace(",", ""))
                    else:
                        LogIt("a", "No Failures for " + _storeNo + ", " + str(code))
            if _fail_count != 0:
                LogIt("R", Rep + ", " + _storeNo + ", " + str(code[0:2]) + " has " + str(_fail_count) + " failures,"
                      + str(set(_failVal)))
                LogIt("r", Rep + "," + _storeNo + "," + str(code[0:2]) + "," + str(_fail_count) + ","
                      + str(set(_failVal)))
            _failVal.clear()
        data.clear()
    except IndexError as e:
        # For cases where the required index is out of range in lists, which can be ignored.
        # LogIt("a", rec)
        LogIt("a", "RepFails Error : Index Error" + str(e))
    except ValueError as e:
        # For cases where fields may not have the correct value for int conversion, which can be ignored
        LogIt("a", "RepFails Error : Value Error" + str(e))
    except Exception as e:
        LogIt("e", "RepFails Error : " + str(e))


# Execution

# Open files required
ActivityLog = FileOpen(FixedPath + LogFile, "w+")
ResultLog = FileOpen(FixedPath + ResultFile, "w+")
ReportLog = FileOpen(FixedPath + ReportFile, "w+")
LogIt("a", "Initializing application by " + getpass.getuser())

# Log Header
LogIt("R", "------------------------------------------------------------------------------------------------")
LogIt("R", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
LogIt("R", "Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("R", "------------------------------------------------------------------------------------------------")


# Mainline
# ClearTxns()
LogIt("r", 'FILE,TOTAL_RECORDS,STORE_NO,RESP_RECORD,RESP_FIELD,FIELD_VALUES')
LogIt("R", 'FILE,TOTAL_RECORDS,STORE_NO,REQ/RSP,TOTAL_FAILURES')
ProcFiles(FixedPath)

# GetData(FixedPath + FixedName)


# Log Footer
LogIt("R", "------------------------------------------------------------------------------------------------")
LogIt("R", "Completed at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("R", "------------------------------------------------------------------------------------------------")

# Log program runtime
RunTime = datetime.datetime.now() - RunTime
LogIt("a", "Program completed successfully in " + str(RunTime))

# Close opened files
FileClose(ActivityLog)
FileClose(ReportLog)
os.system('pause')
