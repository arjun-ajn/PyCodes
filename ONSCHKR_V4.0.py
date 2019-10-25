"""
Program     :   ONSCHKR
Developer   :   Arjun Muraleedharan
Date        :   20-03-2019

Objective:
Check the ONS Suite status in stores reported, by checking the OK files.

Initial Version V1.0
Get the data from OK files and check for anomalies.

Version 2.0
Checks are made for CIPOK and IMFOK suites for testing.

Version 3.0
All suites added and flags are checked for anomalies.

Version 4.0
The program now checks for all the suites except for POGOK & CEFOK

"""

# Initialize
import datetime
import os
import importlib
import glob
import fileinput
import linecache
import sys

paks = ['pandas', 'datetime', 'getpass', 'os', 'sys', 'importlib']
FixedPath = "C:/EPOS/SEARCH/ONE/"
# FixedName = "ONS.txt"
Resultfile = "ResultLog.txt"
LogFile = "ActivityLog.txt"
Reportfile = "ReportLog.csv"

# Error Handling Logs
# Open Log Files
ActivityLog = open(os.path.join(FixedPath ,LogFile), "w+")
ResultLog = open(FixedPath + Resultfile, "w+")


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
                          + "\n" + str(exc_obj)
                          + "\n")
        LogIt("a", err)
        LogIt("p", err)
        exit()
    else:
        print("INVALID LOG TYPE" + "\n")


# Install missing packages
def InstallPacks(pypack):
    os.system('SET HTTPS_PROXY=https://165.225.104.32:9400')
    os.system('SET HTTP_PROXY=http://165.225.104.32:9400')
    LogIt("a", pypack + " Installation : Proxy Set")
    try:
        os.system(sys.executable[:-10] + 'Scripts\pip' + ' install'
                                                     ' --proxy=http://gateway.zscaler.net:9400 --trusted-host pypi.org'
                                                     ' --trusted-host files.pythonhosted.org ' + pypack)
        LogIt("a", pypack + " Installation : Completed")
    except Exception as e:
        LogIt("a", pypack + " Installation Error : " + str(e))


for pak in paks:
    try:
        LogIt("a", "Checking : " + pak)
        importlib.util.find_spec('sys')
        LogIt("a", "Exists : " + pak)
    except ImportError as e:
        LogIt("a", e.name + " : Missing")
        InstallPacks(e.name)
    except Exception as e:
        LogIt("e", "Pak Check Error : " + str(e))
    LogIt("a", "Completed : " + pak)

import getpass


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
        LogIt("e", "Dir Fix Error : " + str(e))


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


# Get the Ok files details
def GetOK():
    LogIt("a", "GetOK : Initializing")
    try:
        count_proc = 0
        count_skip = 0
        if glob.glob(FixedPath + "STORE*.*"):
            for line in fileinput.input(glob.glob(FixedPath + "STORE*.*")):
                # LogIt("p", line)
                # FILTER FILE TYPE TO GET RIGHT DATA
                if fileinput.filename()[-3:] in suites:
                    # LogIt("a", "Processing : " + fileinput.filename()[19:-4] + "," + fileinput.filename()[-3:] + "\n")
                    LogIt("r", fileinput.filename()[19:-4] + "," + fileinput.filename()[-3:] + "," + line.rstrip('\n'))
                    count_proc = count_proc + 1
                else:
                    LogIt("a", "GetOK : SKIPPING : " + fileinput.filename())
                    fileinput.nextfile()
                    count_skip = count_skip + 1
            LogIt("a", "GetOK : Processed " + str(count_proc) + "files and skipped " + str(count_skip) + "files")
            LogIt("a", "GetOK : Completed")
            CheckOk()
        else:
            LogIt("a", "GetOK : No Files to process")
    except Exception as e:
        LogIt("a", "GetOK : Failed : " + str(e))
        LogIt("e", e)
    # FileClose(ResultLog)


def CheckRpt(r, R):
    try:
        if len(r) > 13:
            # REPORT A STORE ONLY IF ANY ISSUES
            R.append(r)
    except Exception as e:
        LogIt("e", e)


# Check the details in the OK files with layouts
def CheckOk():
    LogIt("a", "CheckOk : Initializing")
    rprt = []
    try:
        with open(os.path.join(FixedPath, Resultfile)) as f:
            for line in f:
                rpt = ""
                # LogIt("a", "CURRENT LINE : " + line)
                if line.split(',')[1] == "IMF":
                    rpt = line.split(',')[0] + "," + line.split(',')[1]
                    if line.split(',')[2][:1] != "7":
                        rpt = rpt + ",Run Failed"
                    if int(datetime.datetime.now().strftime("%y%m%d")[2:6]) - int(str(line.split(',')[2][1:]).rstrip()) != 1:
                        rpt = rpt + ",Outdated"
                    CheckRpt(rpt, rprt)
        
                elif line.split(',')[1] == "CIP":
                    rpt = line.split(',')[0] + "," + line.split(',')[1]
                    rpt = rpt + ",Serial : " + line.split(',')[2][4:8]
                    if line.split(',')[2][35] != "E":
                        rpt = rpt + ",Run Failed"
                    if str(line.split(',')[2][8:16]).rstrip() != str(datetime.datetime.now().strftime("%Y%m%d")):
                        rpt = rpt + ",Outdated"
                    if line.split(',')[2][16] != "E":
                        rpt = rpt + ",PSD66 is " + line.split(',')[2][16]
                    if line.split(',')[2][31] != "E":
                        rpt = rpt + ",PSD67 is " + line.split(',')[2][31]
                    if line.split(',')[2][35] != "E":
                        rpt = rpt + ",Status is " + line.split(',')[2][35]
                    if line.split(',')[2][51] != "E":
                        rpt = rpt + ",PSD69 is " + line.split(',')[2][51]
                    if line.split(',')[2][66] != "E":
                        rpt = rpt + ",PSD76 is " + line.split(',')[2][66]
                    CheckRpt(rpt, rprt)
        
                elif line.split(',')[1] == "OBP" or line.split(',')[1] == "JOB":
                    rpt = line.split(',')[0] + "," + line.split(',')[1]
                    if str(line.split(',')[2][6:10]).rstrip() != str(datetime.datetime.now().strftime("%m%d")):
                        rpt = rpt + ",Outdated "
                    if line.split(',')[2][1] != "E":
                        rpt = rpt + ",PSB21 Failed " + line.split(',')[2][1]
                    if line.split(',')[2][2] != "E":
                        rpt = rpt + ",PSB22 Failed " + line.split(',')[2][2]
                    if line.split(',')[2][3] != "E":
                        rpt = rpt + ",PSB23 Failed " + line.split(',')[2][3]
                    if line.split(',')[2][4] != "E":
                        rpt = rpt + ",PSB24 Failed " + line.split(',')[2][4]
                    if line.split(',')[2][5] != "E":
                        rpt = rpt + ",PSB25 Failed " + line.split(',')[2][5]
                    if line.split(',')[2][10] != "0":
                        rpt = rpt + ",Status is " + line.split(',')[2][10]
                    if line.split(',')[2][11] != "E":
                        rpt = rpt + ",PSB27 Failed " + line.split(',')[2][11]
                    if line.split(',')[2][12] != "E":
                        rpt = rpt + ",PSB28 Failed " + line.split(',')[2][12]
                    # if line.split(',')[2][13] != "E":
                    #     rpt = rpt + ",EXACT Failed " + line.split(',')[2][13]
                    # if line.split(',')[2][14] != "E":
                    #     rpt = rpt + ",WBEING Failed " + line.split(',')[2][14]
                    # if line.split(',')[2][15] != "E":
                    #     rpt = rpt + ",PSD87 is " + line.split(',')[2][15]
                    # if line.split(',')[2][16] != "M":
                    #     rpt = rpt + ",IUF Source is " + line.split(',')[2][16]
                    CheckRpt(rpt, rprt)
        
                elif line.split(',')[1] == "DEL":
                    rpt = line.split(',')[0] + "," + line.split(',')[1]
                    if str(line.split(',')[2][13:19]).rstrip() != str(datetime.datetime.now().strftime("%m%d")):
                        rpt = rpt + ",DELOK Run Date : " + str(line.split(',')[2][13:19])
                    rpt = rpt + ",DEALX : " + str(line.split(',')[2][5:9])
                    rpt = rpt + ",DVCHR : " + str(line.split(',')[2][9:13])
                    rpt = rpt + ",DIDIR : " + str(line.split(',')[2][47:51])
                    CheckRpt(rpt, rprt)
        
                elif line.split(',')[1] == "FLT":
                     rpt = line.split(',')[0] + "," + line.split(',')[1]
                     if str(line.split(',')[2][7:11]).rstrip() != str(datetime.datetime.now().strftime("%m%d")):
                         rpt = rpt + ",Run Date : " + str(line.split(',')[2][5:11])
                     if line.split(',')[2][15] != "E":
                         rpt = rpt + ",Status is " + line.split(',')[2][15]
                     CheckRpt(rpt, rprt)
        
                elif line.split(',')[1] == "RFO":
                    rpt = line.split(',')[0] + "," + line.split(',')[1]
                    if line.split(',')[2][0] != "E":
                        rpt = rpt + ",RFAUDIT Failed " + line.split(',')[2][0]
                    if line.split(',')[2][1] != "E":
                        rpt = rpt + ",RFMAINT Failed " + line.split(',')[2][1]
                    if line.split(',')[2][2] != "E":
                        rpt = rpt + ",RFPIKMNT Failed " + line.split(',')[2][2]
                    if line.split(',')[2][3] != "E":
                        rpt = rpt + ",RFCCMNT Failed : Check RF/NONRF " + line.split(',')[2][3]
                    if line.split(',')[2][4] != "E":
                        rpt = rpt + ",RFSCC Failed : Check RF/NONRF" + line.split(',')[2][4]
                    CheckRpt(rpt, rprt)

                elif line.split(',')[1] == "CEF":
                    continue
                    # LogIt("a", "CEFOK : Coming Soon")

                elif line.split(',')[1] == "POG":
                     continue
                     # LogIt("a", "POGOK : Coming Soon")
        
                else:
                    LogIt("p", "Incorrect File Type " + line.split(',')[1])
            LogIt("a", "CheckOK : Completed")
            ReportOK(rprt)
        rprt.clear()
        FileClose(f)
    except Exception as e:
        # LogIt("a", "CheckOK : Failed : " + line + str(e))
        LogIt("e", e)


# Summarize data
def ReportOK(r):
    LogIt("a", "ReportOK : Initializing")
    try:
        # print(r)
        r.sort()
        ReportLog = FileOpen(os.path.join(FixedPath, Reportfile), "w+")
        # ReportLog = open(os.path.join(FixedPath, Reportfile), "w+")
        ReportLog.write("STORE,SUITE,FLAG1,FLAG2,FLAG3,FLAG4,FLAG5,FLAG6\n")
        for line in r:
            # print(line)
            if str(line).rstrip() != "":
                # print("Writing " + Report)
                ReportLog.write(str(line) + "\n")
        FileClose(ReportLog)
        LogIt("a", "ReportOK : Completed")
    except Exception as e:
        # LogIt("a", "ReportOK : Failed : " + str(e))
        LogIt("a", "ReportOK : Failed : ")
        LogIt("e", e)


# Mainline

# Check Python Version & Packages
# LogIt("a", str(os.system(os.environ['HOMEPATH'] + '\AppData\Local\Programs\Python\Python37\python --version')))

LogIt("a", "Initializing application by " + getpass.getuser())
# DirCheck()
suites = ['IMF', 'CIP', 'RFO', 'POG', 'OBP', 'JOB', 'DEL', 'END', 'CEF', 'FLT']
RunTime = datetime.datetime.now()

GetOK()

# Log program runtime
RunTime = datetime.datetime.now() - RunTime
LogIt("a", "Program completed successfully in " + str(RunTime))

# Close opened files
FileClose(ActivityLog)
FileClose(ResultLog)
os.system('pause')
