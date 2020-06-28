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
import glob
from ftplib import FTP
import sys
import importlib
import configparser

# Initializing variables
RunTime = datetime.datetime.now()
data = []
result = []
FormatStr = ["\"UOD|", "\""]
FixedPath = "C:/EPOS/HUMSS/"
FileName = "humss.txt"
FTPDir = "FTP/"
FindStr = ["H,", "CRTLF;"]
FoundStr = [0, 0, 0]
LogFile = "HUMSS_Log.txt"
ResultFile = "HUMSS_Result.txt"
ReplayFile = "HUMSS_Replay.txt"
'''
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
'''
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


# Clean up existing FTP files
def cleanFTP():
    _initFTPfiles = glob.glob(os.path.join(FixedPath, FTPDir) + '*')
    if len(_initFTPfiles) > 0:
        LogIt("a", "Cleaning up files : " + str(_initFTPfiles))
        for ftpfile in _initFTPfiles:
            os.remove(ftpfile)
    else:
        LogIt("a", "No files to clear")


# Get records from input file
def GetRecords():
    try:
        with open(FixedPath+FileName, encoding="ANSI") as f:
            record_count = 0
            for record in f:
                # print(record)
                data.append(record)
                # ResultLog.write(record)
                record_count += 1
            ActivityLog.write("Processed " + str(record_count) + " lines from " + FixedPath+FileName + "\n")
            # print(data)
            PrepRecords(data)
        f.close()
    except IOError:
        ActivityLog.write('An error occured trying to read the file.\n')
    except ValueError as er:
        ActivityLog.write('Non-numeric data found in the file : ' + str(er) + '\n')
    except ImportError:
        ActivityLog.write("NO module found\n")
    except EOFError:
        ActivityLog.write('Why did you do an EOF on me?\n')
    except KeyboardInterrupt:
        ActivityLog.write('You cancelled the operation.\n')
    except:
        ActivityLog.write("Unhandled GetRecord Error\n")


def PrepRecords(data):
    try:
        ActivityLog.write("Prepping Records\n")
        for record in data:
            # print(record)
            LookRecords(record)
            # print("FOUND AT:" + str(record.find(FindStr[0])) + " and " + str(record.rfind(FindStr[1])))
            # result.append(rec)
            # print(result + "\n")
        # ResultLog.write(str(result))
        ActivityLog.write("Clearing data\n")
        data.clear()
        # Now sort the data to group all records of same stores together
        SortIt(result)
    except Exception as e:
        ActivityLog.write("Unhandled PrepRecords Error\n")


def LookRecords(rec):
    FoundStr = [0, 0, 0]
    ActivityLog.write("Looking for 'H,' and 'CRTLF;'.\n")
    FoundStr[0] = rec.find(FindStr[0])  # Find header
    while FoundStr[2] != -1:
        FoundStr[2] = rec.find(FindStr[0], FoundStr[0] + 1, len(rec))  # Find header again
        if FoundStr[2] != -1:  # if another header is found
            print("Found another record.\n")
            ActivityLog.write("Found another record.\n")
            # to get the correct position, we need to add the previous position value
            FoundStr[1] = rec.rfind(FindStr[1], FoundStr[0], FoundStr[2])
            record = rec[FoundStr[0]:FoundStr[1]]  # Get full record
            FoundStr[0] = FoundStr[2]
            FormatRecords(record)
        else:
            # rec = rec[FoundStr[0]:FoundStr[1] + len(str(FoundStr[1]))]
            ActivityLog.write("Single record.\n")
            FoundStr[1] = rec[FoundStr[0]:FoundStr[2]].rfind(FindStr[1]) + FoundStr[0]
            rec = rec[FoundStr[0]:FoundStr[1]]  # Get full record
            FormatRecords(rec)
            ActivityLog.write("Clearing pointers\n")
            FoundStr[2] = -1


def FormatRecords(rec):
    ActivityLog.write("Adding suffix and prefix to each record\n")
    rec = FormatStr[0] + rec + FindStr[1] + FormatStr[1]
    ActivityLog.write("writing the final data to file\n")
    # ResultLog.write(str(rec) + "\n")
    result.append(rec + "\n")


def SortIt(queue):
    try:
        '''
        replay = []
        ResultLog = FileOpen(FixedPath + ResultFile, "r")
        with open(os.path.join(FixedPath + ResultFile)) as ResultLog:
            for rec in ResultLog:
                replay.append(rec)
        # Now sort the records to group all records of each store together
        replay.sort()
        '''
        queue.sort()
        # Close the file to save the
        # FileClose(ResultLog)
        with open(FixedPath + ReplayFile, "w+") as ReplayLog:
            # ReplayLog.writelines(queue)
            for rec in queue:
                ReplayLog.write(str(rec))
        '''
        ReportLog = FileOpen(FixedPath + ReplayFile, "w+")
        for rec in replay:
            ReportLog.write(str(rec))
        # FileClose(ReportLog)
        '''
        for rec in queue:
            FTPFile = rec[9:13] + ".txt"
            with open(os.path.join(FixedPath, FTPDir, FTPFile), 'a') as FTPLogs:
                FTPLogs.write(rec)
        # FileClose(ReportLog)
    except Exception as e:
        LogIt("e", str(e))


# FTP the file to store
def SendIt():
    try:
        conf = input("START FTP TO STORES - Y/N ? : ").upper()
        if conf == "Y":
            LogIt("a", "CONFIRMED TRANSFER : " + conf)
            os.chdir(os.path.join(FixedPath, FTPDir))
            HUMSSStores = glob.glob('*.txt')
            for file in HUMSSStores:
                host = file[:-4]
                if host.count != 4:
                    host = "0000{}".format(host)
                    host = host[-4:]
                host = "S{}A".format(host)

                localFile = "replay.txt"
                # localFile = os.path.join(FixedPath, FTPDir, file)
                LogIt("a", "connecting to %s" % host)

                try:
                    ftp = FTP(host)
                    ftp.login('root', 'd18dev')
                    ftp.cwd("c:/temp/")
                    LogIt("a", "connected to %s" % host)
                    with open(file, 'rb') as transFile:
                        ftp.storbinary('STOR %s' % localFile, transFile, 1)
                    LogIt("a", "Transfered %s to %s" % (file, host))
                    # command = "adxstart ADX_UPGM:SSC01.286 {}".format(localFile)
                    # print("Executing \"%s\" on %s" % (command, host))
                    # ftp.voidcmd(command)
                    ftp.close
                except Exception as e:
                    LogIt("a", "Unable to transfer to %s" % host)
        else:
            LogIt("a", "FILE TRANSFER DENIED")
    except Exception as e:
        LogIt("e", str(e))

# Execution

# Open files required
# ActivityLog = FileOpen(FixedPath + LogFile, "w+")
# ResultLog = FileOpen(FixedPath + ResultFile, "w+")


with open(FixedPath + LogFile, "w+") as ActivityLog:
    with open(FixedPath + ResultFile, "w+") as ResultLog:
        ActivityLog.write("Initializing application by " + getpass.getuser() +
                          " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

        # Mainline
        cleanFTP()
        GetRecords()
        # FileClose(ResultLog)
        # SortIt()
        SendIt()

        # Log program runtime
        RunTime = datetime.datetime.now() - RunTime
        ActivityLog.write("Program completed in " + str(RunTime) + "\n")

# Close opened files
# FileClose(ActivityLog)
# FileClose(ResultLog)
os.system('pause')
