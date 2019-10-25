"""

Program     :   TrendIT
Developer   :   Arjun Muraleedharan
Date        :   20-07-2019

Description:

Functions:

Prerequisites:


Initial Version V1.0
Plot a sample graph

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
if config.read('trendconfig.ini') == 0:
    config.read('trendconfig.ini')
    FixedPath = "D:/DS/EPOS/TREND/"
    FixedName = "incident.xlsx"
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

Today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
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


# GET DATA
def GetData(f):
    df = pd.read_excel(f)
    # CleanData(df)


# CLEAN DATA
def CleanData(df):
    df.columns = [column.replace(" ", "_") for column in df.columns]
    df['Opened_Month'] = df['Opened'].dt.month
    MineData(df)


# MINE DATA
def MineData(df):
    pl = pd.pivot_table(
        df[
            (df['Opened_Month'] >= Today.month - 2)
            & (df['Opened_Month'] <= Today.month - 1)
            ],
        index='Configuration_item',
        columns='Opened_Month',
        values='Priority',
        aggfunc=np.count_nonzero
    )
    pl = pl.fillna(0)
    pl.rename(columns=lambda x: calendar.month_name[x], inplace=True)
    pl['Total'] = pl.sum(axis=1)
    pl.sort_values("Total", axis=0, ascending=False, inplace=True, na_position='last')
    pl.drop('Total', axis=1, inplace=True)
    pl.to_excel(FixedPath + 'Trend.xlsx')
    p = pl.plot.bar()
    p.get_figure().savefig(FixedPath + 'Trend.png', bbox_inches='tight', pad_inches=0)


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
GetData(os.path.join(FixedPath, FixedName))


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
