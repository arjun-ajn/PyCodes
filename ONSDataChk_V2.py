"""
Program     :   ONSDataChk
Developer   :   Arjun Muraleedharan
Date        :   31-07-2019
Description:
Summarize the reports from multiple excel sheets to one
Initial Version V1.0
Initial version to check only one field validation
"""

# Libraries
import datetime
import getpass
import os
import linecache
import sys
import pandas as pd
import numpy as np

RunTime = datetime.datetime.now()
FixedPath = "C:/EPOS/ONS/"
FixedName = "ONS Data Collection.xlsx"
Resultfile = "ResultLog.txt"
LogFile = "ActivityLog.txt"
# SET DEFAULT TIME FOR TODAY
Today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
NaDate = datetime.datetime.today().replace(year=1990)


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
        os.system('pause')
        exit()
    else:
        print("INVALID LOG ERROR!")
        os.system('pause')
        exit()


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


# Grab data from file
def GetData(f):
    try:
        # PULL DATA FROM EXCEL FILE
        LogIt("a","GetData : Extracting " + f)
        df = pd.ExcelFile(f)
        df = df.parse(df.sheet_names[0])
        LogIt("a", "GetData : Completed")
        CleanData(df)
    except Exception as e:
        LogIt("e", "GetData Error : " + str(e))


# Clean Data
def CleanData(df):
    try:
        # PREPARE DATA FOR PROCESSING
        LogIt("a", "CleanData : Initiating")
        # ELIMINATE WHITE SPACES
        df.columns = [column.replace(" ", "_") for column in df.columns]
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='Epos', value='EPOS', regex=True)
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='epos', value='EPOS', regex=True)
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='Apps', value='EPOS', regex=True)
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='apps', value='EPOS', regex=True)
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='APPS', value='EPOS', regex=True)
        df['Actions_Taken'] = df['Actions_Taken'].replace(to_replace='Passed', value='passed', regex=True)
        df['WeekOfYear'] = df["DATE"].dt.week
        LogIt("a", "CleanData : Completed")
        RepData(df)
    except Exception as e:
        LogIt("e", "CleanData Error : " + str(e))


# Report Details
def RepData(df):
    try:
        # EXTRACT VALUES FOR REPORTING
        LogIt("a", "RepData : Initiating")

        # REPORT INVESTIGATIONS
        LogIt("r", "======================== ONS SUMMARY ========================"
              + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        cols = ["DATE", "INC_Number", "Store_Number", "Package", "ONS_Suite", "Issue", "Actions_Taken"]
        with pd.ExcelWriter(FixedPath + "OUT.xlsx") as writer:
            # WRITE THE ACTUAL DATA
            # df.to_excel(writer, sheet_name='DATA', index=False)
            df = df[
                df["DATE"].dt.year == 2019]
                # df['WeekOfYear'] == Today.isocalendar()[1]]
            df.to_excel(writer, sheet_name='DATA', index=False)

            # GET NA COUNTS
            for col in cols:
                xl = df[
                    df[col].isna()]
                if xl.shape[0] > 0:
                    xl.to_excel(writer, sheet_name=col + ' NAs - ' + str(xl.shape[0]), index=False)
                LogIt("r", col + " Blanks : " + str(xl.shape[0]))

            # ONS Suite Validation
            xl = df[
                df["ONS_Suite"].str.len() > 5]
            xl.to_excel(writer, sheet_name='SUITE ERR - ' + str(xl.shape[0]), index=False)
            LogIt("r", "ONS Suite Errors : " + str(xl.shape[0]))

            # Update Validator
            xl = df[
                df["Actions_Taken"].str.find("EPOS") > 0]
            xl.to_excel(writer, sheet_name='EPOS UPDATES - ' + str(xl.shape[0]), index=False)
            LogIt("r", "EPOS Updates: " + str(xl.shape[0]))

            xl = df[
                df["Actions_Taken"].str.find("passed") > 0]
            xl.to_excel(writer, sheet_name='UPDATE ERR - ' + str(xl.shape[0]), index=False)
            LogIt("r", "Actions Taken Incomplete : " + str(xl.shape[0]))

        LogIt("a", "RepData : Completed")
    except Exception as e:
        LogIt("e", "RepData Error : " + str(e))


# Execution
# Open files required
ActivityLog = FileOpen(FixedPath + "ActivityLog.txt", "w+")
ResultLog = FileOpen(FixedPath + "ResultLog.txt", "w+")
LogIt("a", "Initializing " + os.path.basename(sys.argv[0]) + " by " + getpass.getuser())

# Log Header
LogIt("r", "------------------------------------------------------------------------------------------------")
LogIt("r", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
LogIt("r", "Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("r", "------------------------------------------------------------------------------------------------")

GetData(FixedPath + FixedName)

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