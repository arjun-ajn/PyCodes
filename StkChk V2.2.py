"""

Program     :   StkChk
Developer   :   Arjun Muraleedharan
Date        :   11-12-2019

Description:
A script that will give all the stock movement details in the available ADQ2CE files.

Initial Version V1.0
Initial version only gets the store, timeframe, txn, till, type 6 & it's corresponding 22 record from each file.

Version V2.0
Renaming the program to StkChk as the aim is to summarize stock movements of an item.

Version V2.1
Amending code to retrieve the stock details for the mentioned items only. The output will be in the format
STORE,TIMEFRAME:ITEMH:STOCKH

Version V2.2
Amending the code to accept an input excel sheet, get the time frame and item list to check.

"""

# Libraries
import datetime
import getpass
import os
import linecache
import importlib
import sys
import configparser
import xlrd


# Initializing variables
RunTime = datetime.datetime.now()
paks = ['pandas']
data = []


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
    else:
        print("INVALID LOG ERROR!")
        os.system('pause')


# Initialize variables from configuration
try:
    config = configparser.ConfigParser()
    if os.path.exists('StkConfig.ini') and config.read('StkConfig.ini') != 0:
        # config.sections()
        # LogIt("a", "Setting variables from configurations file")
        FixedPath = config.get('inputfiles', 'FixedPath')
        InputFile = config.get('inputfiles', 'InputFile')
        FixedName = config.get('inputfiles', 'FixedName')
        SearchFile = config.get('inputfiles', 'SearchFile')
        ResultFile = config.get('outputfiles', 'ResultFile')
        LogFile = config.get('outputfiles', 'LogFile')
        ReportFile = config.get('outputfiles', 'ReportFile')
        itmH = config.get('searchvalues', 'itemHeader').split(',')  # header record to identify item
        stkH = config.get('searchvalues', 'stkHeader').split(',')  # header record to identify stock value
        items = config.get('searchvalues', 'item').split(',')  # items if required
        if itmH == '':
            itmH = 6
        if stkH == '':
            stkH = 22
        if items == ['']:
            items = []
    else:
        # LogIt("a", "Setting variables from code")
        FixedPath = "C:/EPOS/StkChk/"
        InputFile = "inputfile.xlsx"
        FixedName = "DQ2CE.BIN"
        SearchFile = "ADQ2CE."
        LogFile = "ActivityLog.txt"
        ResultFile = "ResultLog.txt"
        ReportFile = "ReportLog.csv"
        itmH = [6]
        stkH = [20]
except Exception as e:
    LogIt("a", "Configuration Error : " + str(e))


# Install missing packages
def InstallPacks(pypack):
    LogIt("a", pypack + " Installation : Installing " + pypack)
    try:
        os.system(sys.executable[:-10] + 'Scripts\pip' + ' install' + pypack)
    except Exception as e:
        LogIt("a", pypack + " Installation Error : " + str(e))
    LogIt("a", pypack + " Installation : Completed")


# Check for required packages
def PackCheck():
    for pak in paks:
        try:
            LogIt("a", "Checking : " + pak)
            importlib.util.find_spec(pak)
            LogIt("a", "Exists : " + pak)
        except ImportError as e:
            LogIt("a", e.name + " : Missing")
            InstallPacks(e.name)
        except Exception as e:
            LogIt("e", "PackCheck Error : " + str(e))
        LogIt("a", "Completed : " + pak)


# Ensure all directories are in place and create missing ones
def DirCheck():
    LogIt("a", "Checking if directory structure looks all right")
    try:
        Result = os.path.isdir(FixedPath)
        if Result == False:
            LogIt("a", "Directory Structure missing")
            DirFix("ONS")
        else:
            LogIt("a", "Directory structure present")
    except Exception as e:
        LogIt("a", "Dir Check Error : " + str(e))


# Handle missing directories
def DirFix(Name):
    LogIt("a", "Creating missing directories")
    try:
        if (Name == "ONS"):
            os.makedirs(os.path.join(FixedPath, Name))
        else:
            LogIt("a", "Invalid directory is passed")
    except Exception as e:
        LogIt("e", "Dir Fix Error : " + str(e))


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


# Process each file in the folder
def ProcFiles(d):
    try:
        # global Rep
        data.clear()
        LogIt("a", "ProcFiles : Initializing")
        record_count = 0
        LogIt("a", "ProcFiles : Looking up DQ2CE files in " + d)
        for root, dirs, files in os.walk(d):
            for f in files:
                if f.find(SearchFile) == 0:
                    LogIt("a", "ProcFiles : Processing " + root + f)
                    # Get the records from the respective file
                    GetData(d + f, items)
                    record_count += 1
        LogIt("a", "ProcFiles : Processed " + str(record_count) + " files.")
        LogIt("a", "ProcFiles : Completed")
        '''
        for sh in xlrd.open_workbook(FixedPath + InputFile).sheets():
            # print(sh.name)
            for row in range(sh.nrows):
                for col in range(sh.ncols):
                    if 15 <= row <= 158 and col == 6:
                        # print(str(sh.cell(row, 6))[-8:-1])
                        items.append(str(sh.cell(row, 6))[-8:-1])
        '''
        CheckRecords(data, items)
    except Exception as e:
        LogIt("e", "ProcFiles Error : " + str(e))


# Fetch data from each file
def GetData(file, Items):
    # global Rep
    # Fetch all records from the FixedName
    try:
        LogIt("a", "GetData : Extracting " + file)
        # Open and get Details from FixedName
        with open(file) as f:
            record_count = 0
            for record in f:
                # check if there are items specified
                if len(Items) != 0:
                    # check if there are any items in the record
                    if any(',' + _item + ',' in record for _item in Items):
                        # append the record which has an item
                        data.append(record.rstrip())
                else:
                    # if there are no items defined, fetch all records
                    LogIt("a", "No items defined, fetching all data : " + str(record_count))
                    data.append(record.rstrip())
                # count the total records processed
                record_count += 1
            # log the records processed on each file
            LogIt("a", "File - Records : " + str(f.name) + " - " + str(record_count))
        # Log the total records collected
        LogIt("a", "GetData : Completed. " + str(data.__len__()) + " records collected.")
    except Exception as e:
        LogIt("e", "GetData Error : " + str(e))


# Find Strings
def CheckRecords(InitData, Items):
    try:
        LogIt("a", "CheckRecords : Checking Stock Data")
        record_count = 0  # record counter
        for record in InitData:
            _i = 0  # item header counter
            record_count += 1
            while _i < len(itmH):
                if Items:
                    LogIt("a", "CheckRecords : "+ str(len(items)) + " Items specified")
                    for item in Items:
                        _itemHeaderPos = [i for i in range(len(record))
                                          if record.startswith(';' + itmH[_i] + ',' + str(item) + ',', i)]
                        if _itemHeaderPos:
                            _TypStk = record.find(';' + stkH[_i] + ',', _itemHeaderPos[-1] + 1, len(record))
                            # print(item, itemHeaderPos, _TypStk)
                            if _TypStk != -1:
                                LogIt("a", "Line, ItemH, StockH, ItemP, StockP : " + str(record_count) + ", "
                                      + itmH[_i] + ", " + stkH[_i] + ", " + str(_itemHeaderPos[-1]) + ", " + str(_TypStk))
                                print(record[_itemHeaderPos[-1] + 1:record[:_itemHeaderPos[-1]+12].rfind(',')])
                                LogIt("r", record[9:28] + ':'
                                      + record[_itemHeaderPos[-1] + 1:record[:_itemHeaderPos[-1]+12].rfind(',')] + ':'
                                      + record[_TypStk + 1:record.find(';', _TypStk + 1, len(record)) - 6])
                                # LogIt("r", record[9:28] + ':' + record[_itemHeaderPos[-1] + 1:_TypStk - 6] + ':' +
                                #       record[_TypStk + 1:record.find(';', _TypStk + 1, len(record)) - 6])
                            else:
                                LogIt("a", "No type " + stkH[_i] + " for item " + str(item) +
                                      " at record " + str(record_count))
                            # LogIt("r", record)
                        else:
                            LogIt("a", "No type " + itmH[_i] + " for item " + str(item) +
                                  " at record " + str(record_count))
                else:
                    LogIt("a", "CheckRecords : Items not specified")
                    # Find item header positions to a list
                    _itemHeaderPos = [i for i in range(len(record)) if record.startswith(';'+itmH[_i]+',', i)]
                    # record_count += 1
                    # Loop through the item positions
                    if _itemHeaderPos.__len__() is not 0:
                        # locate stock header from the respective item header
                        _TypStk = record.find(';'+stkH[_i]+',', _itemHeaderPos[0] + 1, len(record))
                        # RepStk(record, items, itmH[_i], stkH[_i])
                        if _TypStk != -1:
                            for pos in _itemHeaderPos:
                                LogIt("a", "Line, ItemH, StockH, ItemP, StockP : " + str(record_count) + ", "
                                      + itmH[_i] + ", " + stkH[_i] + ", " + str(pos) + ", " + str(_TypStk))
                                _TypStk = record.find(';'+stkH[_i]+',', pos + 1, len(record))
                                LogIt("r", record[9:45] + record[pos:_TypStk]
                                      + record[_TypStk:record.find(';', _TypStk + 1, len(record))])
                                # RepStk(record, items, itmH[_i], stkH[_i])
                        else:
                            LogIt("a", "Skipping type "+stkH[_i]+" stock record")
                    else:
                        LogIt("a", "No type " + itmH[_i] + " in record " + str(record_count))
                _i += 1
        # InitData.clear()
        LogIt("a", "LookRecords : Completed processing " + str(record_count) + " records")
    except Exception as e:
        LogIt("e", "LookRecords Error : " + str(e))


# Get inputs from excel
def ProcXL(file):
    try:
        LogIt("a", "ProcXL Checking file : " + file)
        if os.path.exists(file):
            for sh in xlrd.open_workbook(file).sheets():
                # print(sh.name)
                for row in range(sh.nrows):
                    for col in range(sh.ncols):
                        if 15 <= row <= 158 and col == 6:
                            # print(str(sh.cell(row, 6))[-8:-1])
                            items.append(str(sh.cell(row, 6))[-8:-1])
            # CheckRecords(data, items)
            ProcFiles(FixedPath)
        else:
            LogIt("a", "ProcXL Error : " + file + " does not exist")
            LogIt("a", "ProcXL Error : Program will exit now")
            pass
    except Exception as e:
        LogIt("e", "LookRecords Error : " + str(e))


# Execution

# Open files required
ActivityLog = FileOpen(FixedPath + "ActivityLog.txt", "w+")
ResultLog = FileOpen(FixedPath + "ResultLog.txt", "w+")
LogIt("a", "Initializing application by " + getpass.getuser())
# PackCheck()

# Log Header
LogIt("r", "------------------------------------------------------------------------------------------------")
LogIt("r", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
LogIt("r", "Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("r", "------------------------------------------------------------------------------------------------")
LogIt("r", "STORE;H1;ITMH;STKH")

# Initiate processing files
# ProcFiles(FixedPath)
ProcXL(FixedPath+InputFile)

# Log Footer
LogIt("r", "------------------------------------------------------------------------------------------------")
LogIt("r", "Completed at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("r", "------------------------------------------------------------------------------------------------")

# Log program runtime
RunTime = datetime.datetime.now() - RunTime
LogIt("a", "Program completed successfully in " + str(RunTime))

# Close opened files
FileClose(ActivityLog)
FileClose(ResultLog)
os.system('pause')
