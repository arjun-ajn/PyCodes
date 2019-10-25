"""

Program     :   
Developer   :   Arjun Muraleedharan
Date        :   00-00-2019

Description:
This program is meant to do something extraordinary

Initial Version V1.0
The initial version performs a part of the extraordinary function.

"""

# Libraries
import datetime
import getpass
import os
import linecache
import importlib
import sys
import configparser

# Initializing variables
RunTime = datetime.datetime.now()
paks = ['pandas']

config = configparser.ConfigParser()
if config.read('advconfig.ini') == 0:
    config.read('advconfig.ini')
    FixedPath = "C:/EPOS/"
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
        exit()
    else:
        print("INVALID LOG ERROR!")
        exit()


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


# Execution

# Open files required
ActivityLog = FileOpen(FixedPath + "ActivityLog.txt", "w+")
ResultLog = FileOpen(FixedPath + "ResultLog.txt", "w+")
LogIt("a", "Initializing application by " + getpass.getuser())
PackCheck()

# Log Header
LogIt("r", "------------------------------------------------------------------------------------------------")
LogIt("r", os.path.basename(sys.argv[0]) + " by Arjun Muraeedharan")
LogIt("r", "Executed by " + getpass.getuser() + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
LogIt("r", "------------------------------------------------------------------------------------------------")

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
