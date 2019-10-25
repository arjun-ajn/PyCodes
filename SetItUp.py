"""

Program     :   SetItUp
Developer   :   Arjun Muraleedharan
Date        :   31-07-2019

Description:
Run this script after python installation and it will install necessarry packages for a development environment

Initial Version V1.0
Installs packages mentioned in the array

"""

# Libraries
import datetime
import getpass
import os
import linecache
import importlib
import sys

# Initializing variables
RunTime = datetime.datetime.now()
paks = ['pandas', 'numpy']
FixedPath = "C:/EPOS/"
FixedName = ""
Resultfile = "ResultLog.txt"
LogFile = "ActivityLog.txt"
Reportfile = "ReportLog.csv"


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


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


#Upgrade Pip
def PipUpgrade():
    try:
        setDir = 'python '
        setProxy = '--proxy=http://gateway.zscaler.net:9400 '
        setTrust = '--trusted-host pypi.org --trusted-host files.pythonhosted.org '
        if os.system(sys.executable + ' -m pip install ' + setProxy + setTrust + '--upgrade pip') == 0:
            LogIt("a", "PipUpgrade : Upgrade Success")
        else:
            LogIt("a", "PipUpgrade : Upgrade Failed")
        LogIt("a", "PipUpgrade : Completed")
    except Exception as e:
        LogIt("e", "PipUpgrade Error : " + str(e))


# Check for required packages
def PackCheck(paks):
    try:
        for pak in paks:
            LogIt("a", "Checking : " + pak)
            # importlib.util.find_spec(pak)
            importlib.import_module(pak)
            LogIt("a", "Exists : " + pak)
    except ImportError as e:
        LogIt("a", e.name + " : IE Missing")
        InstallPacks(e.name)
    except Exception as e:
        LogIt("e", "PackCheck Error : " + str(e))
    LogIt("a", "PackChk : Completed")


# Install missing packages
def InstallPacks(pypack):
    LogIt("a", "InstallPacks : Installing " + pypack)
    try:
        # os.system(sys.executable[:-10] + 'Scripts\pip' + ' install' + pypack)
        setDir = 'Scripts\pip'
        setProxy = ' --proxy=http://gateway.zscaler.net:9400 --trusted-host pypi.org'
        setTrust = ' --trusted-host files.pythonhosted.org '
        if os.system(sys.executable[:-10] + setDir + ' install' + setProxy + setTrust + pypack) == 1:
            LogIt("a", "InstallPacks : " + pypack + " installation failed")
        LogIt("a", "InstallPacks : " + pypack + " Installation Completed : ")
    except Exception as e:
        LogIt("a", pypack + " Installation Error : " + str(e))
    LogIt("a", pypack + "InstallPacks : Completed")


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

PipUpgrade()
PackCheck(paks)

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
