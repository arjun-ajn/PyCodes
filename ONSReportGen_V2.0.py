"""
Program     :   ONSReportGen
Developer   :   Arjun Muraleedharan
Date        :   20-03-2019

Objective:
Create the data for "ONS Data Collection" report shared with the team daily.

This will save a lot of time invested on making this report. The team will just need to copy the
details from the output file, amend it in the Data collection file and share it with wider team.

Pre requisites:
1. Input file from MyServicePortal in C:/EPOS/ONS/incident.xlsx
	a.Must have Fields in the input file:
		i.	Updated
		ii.	Number
		iii.	Affected User/Store
		iv.	Closed
		v.	Configuration item
		vi.	Description
		vii.	Closure Notes

Challenges :
1. Python version on end user system
2. Packages availability on en user system

Initial Version V1.0
Import the data from input file and format the data for the output.
Write the formatted data to the output file and handle the missing values.

Version 2.0
Code now checks for the required directory structure
Added code to install missing package in the machine & better exception handling.

"""

# Initialize
import datetime
import os
import imp
import sys

paks = ['pandas', 'datetime', 'getpass', 'os', 'sys', 'imp']
RunTime = datetime.datetime.now()
FixedPath = "C:/EPOS/ONS/"
FixedName = "incident.xlsx"
DataName = "ONS Data Collection.xlsx"
Resultfile = "ResultLog.txt"
LogFile = "ActivityLog.txt"

# Error Handling Logs
# Open Log Files
ActivityLog = open(os.path.join(FixedPath ,LogFile), "w+")
# ResultLog = FileOpen(FixedPath + Resultfile, "w+")

def LogIt(type, txt):
    if type == "a":
        ActivityLog.write(txt + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")
    # elif type == "r":
        # ResultLog.write(txt + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

# Install missing packages
def InstallPacks(pak):
    os.system('SET HTTPS_PROXY=https://165.225.104.32:9400')
    os.system('SET HTTP_PROXY=http://165.225.104.32:9400')
    LogIt("a", pak + " Installation : Proxy Set")
    try:
        LogIt("a", str(os.system(sys.executable[:-10] + 'Scripts\pip' + ' install'
                                                     ' --proxy=http://gateway.zscaler.net:9400 --trusted-host pypi.org'
                                                     ' --trusted-host files.pythonhosted.org ' + pak)))
        LogIt("a", pak + " Installation : Please wait and try again after some time")
    except Exception as e:
        LogIt("a", pak + " Installation Error : " + str(e))

for pak in paks:
    try:
        LogIt("a", " Checking" + pak)
        found = imp.find_module(pak)
        LogIt("a", str(found))
        LogIt("a", pak + " Installed")
    except ImportError as e:
        LogIt("a", e.name + " Missing")
        InstallPacks(e.name)
    except Exception as e:
        LogIt("a", "Pak Check Error : " + str(e))
    LogIt("a", pak + " Checked")

import getpass
import pandas as pd


# Function Definitions

# Ensure all directories are in place and create missing ones
def DirCheck():
    LogIt("a", "Checking if directory structure looks all right")
    try:
        Result = os.path.isdir("C:/EPOS/ONS")
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
            os.makedirs("C:/EPOS/ONS")
        else:
            LogIt("a", "Invalid directory is passed")
    except Exception as e:
        LogIt("a", "Dir Fix Error : " + str(e))

# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f

# Close Files
def FileClose(file):
    file.close()

# Grab data from file
def GetData(FullPath):
    try:
        LogIt("a","Getting Data from " + FullPath)
        cols = ["Updated", "Number", "Affected User/Store", "Closed", "Configuration item", "Description", "Closed",
                "Closed", "Closure Notes"]
        xl = pd.ExcelFile(FullPath)
        xl = xl.parse(xl.sheet_names[0])
        xl = xl[cols]
        FixedName = str(datetime.datetime.now().strftime("%d-%m-%Y")) + ".xlsx"
        WriteData(xl, FixedName)
    except Exception as e:
        LogIt("a", "GetData Error : " + str(e))


# Write data to file
def WriteData(xl, FixedName):
    try:
        xl.columns = ["Date", "INC Number", "Store Number", "Package", "ONS Suite", "Issue", "Files Not Transmitted",
              "Number Of  Days", "Actions Taken"]
        xl["Package"] = "18D"
        xl["Date"] = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        # xl["Date"] = str(datetime.datetime.now().strftime("%d-%m-%Y"))
        xl["Files Not Transmitted"] = ""
        xl["Number Of  Days"] = ""
        LogIt("a", "Writing Data to " + os.path.join(FixedPath, FixedName))
        xl.to_excel(os.path.join(FixedPath, FixedName), index=False)
        LogIt("a", "Updated " + os.path.join(FixedPath, FixedName))
    except Exception as e:
        LogIt("a", "WriteData Error : " + str(e))


# Append data to Data File
def AmendData():
    print("This will amend data")
    df1 = pd.read_excel(FixedPath + FixedName, header=0)
    df2 = pd.read_excel(FixedPath + DataName, header=0)
    df2 = pd.concat([df2, df1])
    pd.DataFrame.to_excel(df2, FixedPath + "today.xlsx", index=0)

# Mainline

# Check Python Version & Packages
LogIt("a", str(os.system(os.environ['HOMEPATH'] + '\AppData\Local\Programs\Python\Python37\python --version')))


LogIt("a", "Initializing application by " + getpass.getuser())
DirCheck()
GetData(FixedPath + FixedName)

# Log program runtime
RunTime = datetime.datetime.now() - RunTime
ActivityLog.write("Program completed in " + str(RunTime))

# Close opened files
FileClose(ActivityLog)
# FileClose(ResultLog)