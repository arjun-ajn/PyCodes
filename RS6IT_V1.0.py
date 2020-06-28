"""

Program     :
Developer   :   Arjun Muraleedharan
Date        :   00-00-2019

Description:
This program is for handling incidents from Richard Gooding so that we don't need to suffer.

Initial Version V1.0
Pulling the items from input file

"""

# Libraries
import datetime
import getpass
import os
import glob
import linecache
import pandas as pd
import sys
import configparser

# Initializing variables
RunTime = datetime.datetime.now()
Qdata = []
RSdata = []

config = configparser.ConfigParser()
if config.read('RS6Config.ini') == 0:
    config.read('RS6Config.ini')
    FixedPath = "C:/EPOS/RS6/"
    FixedName = "input.csv"
    SearchFile = "ADQ2CE."
    LogFile = "ActivityLog.txt"
    ResultFile = "ResultLog.csv"
    ReportFile = "ReportLog.csv"
else:
    # config.sections()
    FixedPath = config.get('inputfiles', 'FixedPath')
    FixedName = config.get('inputfiles', 'FixedName')
    SearchFile = config.get('inputfiles', 'SearchFile')
    ResultFile = config.get('outputfiles', 'ResultFile')
    LogFile = config.get('outputfiles', 'LogFile')
    ReportFile = config.get('outputfiles', 'ReportFile')


# Error Handling Logs
def LogIt(typ, txt):
    if typ == "a":
        ActivityLog.write(str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + " : " + txt + "\n")
        print(txt)
    elif typ == "r":
        ResultLog.write(txt + "\n")
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
        exit()
    else:
        print("INVALID LOG ERROR!")
        exit()


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


# Then get the data from DQ2 files
def GetData():
    # LogIt("a", "This will fetch the Q data")
    # data.clear()
    LogIt("a", "GetQ : Initializing")
    record_count = 0
    file_count = 0
    os.chdir(os.path.join(FixedPath))
    QFiles = glob.glob(SearchFile+'*')
    _items = []
    # First get data from the RS file
    with open(FixedPath + FixedName) as infile:
        for line in infile:
            # Clean the record
            line = line.replace(' EA', '')
            # Append it to the RS data list
            RSdata.append(line)
            # Create an item list for further use
            _items.append(line.split(',')[0][-7:])
    # Then fetch data from the DQ files
    for qfile in QFiles:
        with open(FixedPath + qfile) as file:
            # this file can be skipped if the file is newer than the refreshed time-frame
            for line in file:
                # print(line[14:28], tf)
                if line[14:28] <= tf:
                    # There is not only type 6 but also type 13 to be considered.
                    # Items are nested as type 20 within type 13
                    if any((';6,' + _item) in line for _item in _items) \
                                or any((';20,' + _item) in line for _item in _items):
                        # Consider the line only if there is an item from the request present.
                        # print(line)  # Debug
                        Qdata.append(line.replace('\"', ''))
                        # Count the number of hits
                        record_count += 1
                    # else:
                        # LogIt("a", "No hits for " + str(line[14:28]))
            file_count += 1
    '''
    print(len(_items), _items)
    print(len(Qdata), Qdata)
    print(len(RSdata), RSdata)
    print(record_count)
    print(file_count)
    '''
    PrepData(Qdata, _items)


# Now compare the stock values
def PrepData(_Qdata, _RSitems):
    LogIt("a", "PrepData : Compare them")
    with open(FixedPath+ReportFile, 'w+') as report:
        for _item in _RSitems:
            LogIt("a", "PrepData : Formatting item " + str(_item))
            # For each item in the RSdata
            for line in _Qdata:
                # print(line)
                # For each line which has a hit
                if line.find(';6,' + _item) != -1:
                    # Format the record as TIMEFRAME, ITEM, TSF
                    _timeFrame = line[13:27]
                    # Locate the item position
                    _itemLoc = line.find(';6,' + _item) + 3  # May need to use RFIND here if the last occurrance needs to be checked
                    # Locate the stock position ie. item pos + stock pos, assuming is that all the items will have a 22 against it
                    _stkLoc = _itemLoc + line[_itemLoc:].find(';22,') + 4
                    _itemNumber = line[_itemLoc:line[_itemLoc:].find(',')+_itemLoc]
                    _Tsf = line[_stkLoc: line[_stkLoc:].find(',')+_stkLoc]
                    report.write(_timeFrame + ',' + _itemNumber + ',' + _Tsf + '\n')
                if line.find(';20,' + _item) != -1:
                    # Format the record as TIMEFRAME, ITEM, TSF
                    _timeFrame = line[13:27]
                    # Locate the item position
                    _itemLoc = line.find(';20,' + _item) + 4  # May need to use RFIND here if the last occurrance needs to be checked
                    # Locate the stock position ie. item pos + stock pos, assuming is that all the items will have a 22 against it
                    _stkLoc = _itemLoc + line[_itemLoc:].find(';22,') + 4
                    _itemNumber = line[_itemLoc:line[_itemLoc:].find(',') + _itemLoc]
                    _Tsf = line[_stkLoc: line[_stkLoc:].find(',') + _stkLoc]
                    report.write(_timeFrame + ',' + _itemNumber + ',' + _Tsf + '\n')
                # print(_timeFrame + ',' + _itemNumber + ',' + _Tsf)
    # Using pandas, read the RS6 input file
    R_df = pd.read_csv(FixedPath + FixedName, names=["ITEM", "CVOS", "SAP"])
    # Clean the data
    R_df["CVOS"] = R_df["CVOS"].str.replace(' EA', '')
    R_df["SAP"] = R_df["SAP"].str.replace(' EA', '')
    # Read data created from ADQ2 files
    Q_df = pd.read_csv(FixedPath + ReportFile, names=["TIME", "ITEM", "TSF"])
    ProcData(Q_df, R_df)


# Check the data
def ProcData(Q, R):
    LogIt("a", "ProcData : Comparing TSF with CVoS & SAP.")
    LogIt("r", "ITEM, TSF, CVOS, SAP")
    for _item in R['ITEM'].tolist():
        LogIt("a", "ProcData : Comparing item " + str(_item))
        # Fetch the ADQ2 TSF, CVOS & SAP values of an item
        _qTSF = Q.loc[Q['TIME'] == Q.loc[Q['ITEM'] == _item]['TIME'].max()]['TSF'].tolist()
        _rCVOS = R.loc[R['ITEM'] == _item]['CVOS'].values[0].replace(',', '')
        _rSAP = R.loc[R['ITEM'] == _item]['SAP'].values[0].replace(',', '')
        # To avoid errors, we check if TSF is empty
        # if len(_qTSF) != 0 and len(_rCVOS) != 0 and len(_rSAP) != 0:
        if len(_qTSF) != 0:
            if _qTSF[0] == int(_rCVOS):
                LogIt("r", str(_item) + "," + str(_rCVOS) + "," + "CVOS")
            elif _qTSF[0] == int(_rSAP):
                LogIt("r", str(_item) + "," +  str(_rSAP) + "," + "SAP")
            else:
                # LogIt("a", "NO MATCH FOR ITEM : " + str(_item))
                LogIt("r", str(_item) + "," + str(_qTSF[0]) + "," + str(_rCVOS) + "," + str(_rSAP))
        else:
            LogIt("a", "NO TSF FOR ITEM : " + str(_item))


# Execution

# Open files required
with open(FixedPath + LogFile, "w+") as ActivityLog:
    with open(FixedPath + ResultFile, "w+") as ResultLog:
        ActivityLog.write("Initializing application by " + getpass.getuser() +
                          " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

        LogIt("a", "Initializing application by " + getpass.getuser())

        # Log Header
        LogIt("r", "------------------------------------------------------------------------------------------------")
        LogIt("r", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
        LogIt("r", "Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        LogIt("r", "------------------------------------------------------------------------------------------------")

        # Mainline
        # Collect the time-frame from the RS sheet against "Last Refreshed" cell value ex: 05/04/2020 12:18:24
        LogIt("a", "Collect the time-frame from the RS sheet against 'Last Refreshed' cell value ex: 05/04/2020 12:18:24should be in 'dd/mm/yyyy hh:mm:ss'.")
        tf = input("Enter the time-frame : ")
        # tf = '05/04/2020 12:18:24' # FOR TESTING
        # Note that the input should be in "dd/mm/yyyy hh:mm:ss" so that the conversion will be in the correct format
        tf = tf.replace(':', '').replace('/', '').replace(' ', '')
        tf = tf[4:-6] + tf[2:-10] + tf[:-12] + tf[8:]
        # Validate the time-frame and proceed only if the time'frame is correct
        if len(tf) == 14 and tf.isnumeric():  # These checks may not be enough.
            LogIt("a", "Time-frame EPOS : " + tf)
            GetData()
        else:
            LogIt("a", "Invalid timeframe to check. This program will end.")

        # Log Footer
        LogIt("r", "------------------------------------------------------------------------------------------------")
        LogIt("r", "Completed at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        LogIt("r", "------------------------------------------------------------------------------------------------")

        # Log program runtime
        RunTime = datetime.datetime.now() - RunTime
        LogIt("a", "Program completed successfully in " + str(RunTime))

# Wait user input for exit
os.system('pause')
