"""

Program     :   HUMSSCLR
Developer   :   Arjun Muraleedharan
Date        :   24-01-2019

HUMSS Formatter to format failed HUMSS records sent over in corrupt input file.
Challenges:
1. Corrupt Data - Completed
2. Multiple stores in single record - Completed
3. Same store records are scattered across multiple lines

VERSION 1.0 - Initial Version
Handling corrupt data by getting the proper record by checking the header and delimiter

VERSION 2.0 - Multiple Stores In One Record
Check if multiple Headers are present in one record to confirm only one header is present

VERSION 3.0 - Added Functions & More Reporting
PrepRecords are split in one more function FormatRecords. More logging on operations to understand progress

VERSION 4.0 - Added Logging Functions
New functions are added to make logging easier.


VERSION 5.0 - Sorted stores
Sorting the final data to club records to be replayed in each store.
"""

# Libraries
import datetime
import getpass
import os

# Initializing variables
RunTime = datetime.datetime.now()
data = []
FormatStr = ["\"UOD|", "\""]
FixedPath = "C:/EPOS/HUMSS/"
FileName = "humss.txt"
FindStr = ["H,", "CRTLF;"]
FoundStr = [0, 0, 0]


# Function definitions

# Error Handling
def OnError(Fn, obj):
    if str(obj) == "division by zero":
        print(Fn + str(obj))
    elif str(obj) == "IOError":
        print(Fn + str(obj) + "An error occured trying to read the file")
    elif str(obj) == "ValueError":
        print(Fn + str(obj) + "An error occured trying to read the file")
    elif str(obj) == "ImportError":
        print(Fn + str(obj) + "An error occured trying to read the file")
    elif str(obj) == "EOFError":
        print(Fn + "An error occured trying to read the file")
    elif str(obj) == "KeyboardInterrupt":
        print(Fn + "An error occured trying to read the file")
    else:
        print(Fn + "Unhandled Exception : " + str(obj.args))


# Logs
def LogIt(type, txt):
    if type == "a":
        ActivityLog.write(txt + " at" + datetime.datetime.now() + '\n')
    elif type == "r":
        ResultLog.write(txt + " at" + datetime.datetime.now() + '\n')


# Open Files
def FileOpen(file, mode):
    f = open(file, mode)
    return f


# Close Files
def FileClose(file):
    file.close()


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
            #print(data)
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
    # return rec


def PrepRecords(data):
    # try:
        result = []
        ActivityLog.write("Prepping Records\n")
        for record in data:
            print(record)
            LookRecords(record)
            # print("FOUND AT:" + str(record.find(FindStr[0])) + " and " + str(record.rfind(FindStr[1])))
            # result.append(rec)
            # print(result + "\n")
        # ResultLog.write(str(result))
        ActivityLog.write("Clearing data\n")
        data.clear()
        ActivityLog.write("Clearing results\n")
        result.clear()
    # except:
        # ActivityLog.write("Unhandled PrepRecords Error\n")


def FormatRecords(rec):
    ActivityLog.write("Adding suffix and prefix to each record\n")
    rec = FormatStr[0] + rec + FindStr[1] + FormatStr[1]
    ActivityLog.write("writing the final data to file\n")
    ResultLog.write(str(rec) + "\n")


def SendIt(ftp, file):
    print(ftp.storbinary("STOR " + file, open(file, "rb"), 1024))
    """
    ext = os.path.splitext(file)[1]
    if ext not in (".txt", ".htm", ".html"):
        ftp.storlines("STOR " + file, open(file))
    else:
        ftp.storbinary("STOR " + file, open(file, "rb"), 1024)
    """
def SortIt():
    replay = []
    ResultLog = FileOpen(FixedPath + "ResultLog.txt", "r")
    for rec in ResultLog:
        replay.append(rec)
    replay.sort()
    FileClose(ResultLog)
    ReportLog = FileOpen(FixedPath + "ReportLog.txt", "w+")
    for rec in replay:
        ReportLog.write(str(rec))
    FileClose(ReportLog)


# Execution

# Open files required
ActivityLog = FileOpen(FixedPath + "ActivityLog.txt", "w+")
ResultLog = FileOpen(FixedPath + "ResultLog.txt", "w+")
ActivityLog.write("Initializing application by " + getpass.getuser() +
                  " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + "\n")

# Mainline
GetRecords()
FileClose(ResultLog)
SortIt()

# Log program runtime
RunTime = datetime.datetime.now() - RunTime
ActivityLog.write("Program completed in " + str(RunTime) + "\n")

# Close opened files
FileClose(ActivityLog)
FileClose(ResultLog)
