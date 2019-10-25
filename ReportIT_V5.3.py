"""

Program     :   ReportIT
Developer   :   Arjun Muraleedharan
Date        :   14-05-2019

Description:
Summarize the reports from multiple excel sheets to one

Initial Version V1.0
Initial version to work on just one file and ge the current SEV1/SEV2 in queue.

Version V2.0
The program now checks for INCIDENT data from the xlsx file sent in mail daily morning at 930AM IST. Details regarding
the below are reported:
1.	Yesterday’s SEV 1
2.	Today’s SEV 1
3.	Unassigned
4.	Workable Tickets
5.	Not updated in 2 days
6.	Aged Tickets
7.	Escalated Tickets
8.	How Do Is in past 7 days
9.	Group Counts
a.	EPOS Tickets
b.	Photo Tickets
10.	Status counts
a.	Assigned, work in progress etc.
11.	Severity Counts
12.	Incident Type Counts

Version V3.0
Added functions to process the data for INVESTIGATIONS and write all the results to a separate txt file.
The below features are added:
1.	New Investigations(can be amended as per days to check)
2.	Closed investigations(currently checks latest closed, can be amended as per days to check)
3.	Active investigations in the queue
4.	Not updated for 10 days
5.	Status counts

Version V4.0
Adding and re-organizing reports to match Stats file requirements.
Added separate daily and weekly counts for EPOS & Photo
Removed un-necessary reports

Version V4.1
Changes in cleaning data and age calculation by week added.

Version V5.0
Removing Photo team calculations and will report EPOS Team calculations only

Version V5.2
Tweaking code to get more accurate results. Changed ResultLog write mode to append, for testing purposes.
Added developer info, execution info in header and footer.

Version V5.3
Tweaking code to let the code process Friday's data as well.

"""

# Libraries
import datetime
import getpass
import os
import linecache
import sys
import pandas as pd
import numpy as np

# Variable Decalarations
RunTime = datetime.datetime.now()
FixedPath = "C:/EPOS/REPORT/"
Resultfile = "ResultLog.txt"
LogFile = "ActivityLog.txt"
NaDate = datetime.datetime.today().replace(year=1990)
Today = datetime.datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)


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


def DayCheck():
    try:
        if Today.isoweekday() == 1:
            eow = input("it's a Monday. Proceed with Saturday's data?").upper()
            if eow == "Y":
                Today = Today.replace(day=Today.day - 2)
            else:
                print("Proceeding with Monday's processing")
    except Exception as e:
        LogIt("e", "DayCheck Error : " + str(e))


# Grab data from file
def GetData(f, inType):
    try:
        # PULL DATA FROM EXCEL FILE
        LogIt("a","GetData : Extracting " + f)
        # cols = ['Number', 'Priority', 'Affected_User/Store', 'Assignment_Group',
        #         'Short_Description', 'Configuration_item', 'Status']
        df = pd.ExcelFile(f)
        df = df.parse(df.sheet_names[0])
        if inType == "I":
            LogIt("a", "GetData : INC : Completed")
            CleanData(df, "I")
        elif inType == "P":
            LogIt("a", "GetData : INV : Completed")
            CleanData(df, "P")
        elif inType == "ED":
            RepData(df, "ED")
        elif inType == "PD":
            RepData(df, "PD")
        elif inType == "EW":
            df.columns = [column.replace(" ", "_") for column in df.columns]
            RepData(df, "EW")
        else:
            LogIt("a", "GetData Error : Invalid Type")
    except Exception as e:
        LogIt("e", "GetData Error : " + str(e))


# Clean Data
def CleanData(df, inType):
    try:
        # PREPARE DATA FOR PROCESSING
        LogIt("a", "CleanData : Initiating")
        # ELIMINATE WHITE SPACES
        df.columns = [column.replace(" ", "_") for column in df.columns]
        # SHORTEN GROUP NAME FOR BETTER UNDERSTANDING
        df['Assignment_Group'] = df['Assignment_Group'].replace(to_replace=r'^.*EPOS.*$', value='EPOS', regex=True)
        # HANDLE MISSING VALUES
        df['Closed'] = df['Closed'].fillna(NaDate)
        # CREATE CURRENT/CALCULATION DATE
        df['Today'] = datetime.datetime.today()
        # CONVERT TIME-FRAME TO DAYS
        df['CreatedAge'] = df['Today'] - df['Created']
        df['CreatedAge'] = df['CreatedAge'] / np.timedelta64(1, 'D')
        df['UpdatedAge'] = df['Today'] - df['Updated']
        df['UpdatedAge'] = df['UpdatedAge']/np.timedelta64(1, 'D')
        df['ClosedAge'] = df['Today'] - df['Closed']
        df['ClosedAge'] = df['ClosedAge'] / np.timedelta64(1, 'D')
        # TO INT FOR EASE OF CALCULATION
        df['CreatedAge'] = df['CreatedAge'].astype(int)
        df['UpdatedAge'] = df['UpdatedAge'].astype(int)
        df['ClosedAge'] = df['ClosedAge'].astype(int)
        LogIt("a", "CleanData : Completed")
        # RepINC(df)
        if inType == "I":
            # CONVERT TIME-FRAME TO DAYS
            df['ResolvedAge'] = df['Today'] - df['Resolved']
            df['ResolvedAge'] = df['ResolvedAge'] / np.timedelta64(1, 'D')
            df['CreatedWeek'] = df["Created"].dt.week
            df['Resolved'] = df['Resolved'].fillna(NaDate)
            df['ResolvedWeek'] = df["Resolved"].dt.week
            RepData(df, "I")
        elif inType == "P":
            # CONVERT TIME-FRAME TO DAYS
            df['CreatedWeek'] = df["Created"].dt.week
            df['ClosedWeek'] = df["Closed"].dt.week
            RepData(df, "P")
        else:
            LogIt("a", "CleanData Error : Invalid Type")
    except Exception as e:
        LogIt("e", "CleanData Error : " + str(e))


# Report Details
def RepData(df, inType):
    try:
        # EXTRACT VALUES FOR REPORTING
        LogIt("a", "RepData : Initiating")
        if inType == "I":
            # REPORT INCIDENTS
            LogIt("r", "======================== INCIDENT SUMMARY =============================")
            cols = ['Number', 'Priority', 'Affected_User/Store', 'Assignment_Group', 'Assigned_to',
                    'Short_Description', 'Configuration_item', 'Status', 'Created', 'Updated', 'Resolved', 'CreatedAge',
                    'UpdatedAge', 'ResolvedAge', 'ClosedAge']
            with pd.ExcelWriter("C:/EPOS/REPORT/INC.xlsx") as writer:
                # WRITE THE ACTUAL DATA
                df.to_excel(writer, sheet_name='DATA', index=False)

                # FETCH THE SEVERITY 1/2 COUNTS
                xl = df[cols][
                    (df["Priority"] == 'Sev - 1')
                    & (df["CreatedAge"] >= 1)
                    & (df["CreatedAge"] <= 2)]
                xl.to_excel(writer, sheet_name='YDAY SEV 1 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Yesterday's SEV 1 : " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Priority"] == 'Sev - 2')
                    & (df["CreatedAge"] >= 1)
                    & (df["CreatedAge"] <= 2)]
                xl.to_excel(writer, sheet_name='YDAY SEV 2 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Yesterday's SEV 2 : " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Priority"] == 'Sev - 1')
                    & (df["CreatedAge"] <= 1)]
                xl.to_excel(writer, sheet_name='TODAY SEV 1 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Today's SEV 1 - " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Priority"] == 'Sev - 2')
                    & (df["CreatedAge"] <= 1)]
                xl.to_excel(writer, sheet_name='TODAY SEV 2 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Today's SEV 2 - " + str(xl.shape[0]))

                # FETCH TODAYS UNASSIGNED INCIDENT COUNTS
                xl = df[cols][
                    (df["Assigned_to"].isna())
                    & (df["Active"] == True)]
                xl.to_excel(writer, sheet_name='UNASSIGNED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Unassigned - " + str(xl.shape[0]))

                # FETCH TODAYS INCIDENT COUNTS
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)]
                xl.to_excel(writer, sheet_name='WORKABLE - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Workable - " + str(xl.shape[0]))

                # FETCH NOT UPDATED COUNTS
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)
                    & (df["UpdatedAge"] >= 2)]
                xl.to_excel(writer, sheet_name='NOT UPDATED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Not Updated In 2 Days - " + str(xl.shape[0]))

                # FETCH AGED COUNTS
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)
                    & (df["CreatedAge"] >= 4.5)]
                xl.to_excel(writer, sheet_name='AGED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Aged Incidents Over 5 Days - " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)
                    & (df["CreatedAge"] >= 4.5)
                    & (df["UpdatedAge"] >= 2)]
                xl.to_excel(writer, sheet_name='AGED NO UPD- ' + str(xl.shape[0]), index=False)
                LogIt("r", "Aged & Not Updated Over 2 Days - " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)
                    & (df["CreatedAge"] >= 4.5)
                    & (df["ResolvedAge"] >= 1)
                    & (df["ResolvedAge"] <= 2)]
                xl.to_excel(writer, sheet_name='AGED RES- ' + str(xl.shape[0]), index=False)
                LogIt("r", "Aged & Resolved Yesterday - " + str(xl.shape[0]))

                # OVER 30 DAYS
                xl = df[cols][
                    (df["Status"] != "Resolved")
                    & (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["Active"] == True)
                    & (df["CreatedAge"] >= 30)]
                xl.to_excel(writer, sheet_name='AGED 30 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Aged Incidents Over 30 Days - " + str(xl.shape[0]))

                # FETCH THE HOW DO I COUNTS
                xl = df[cols][
                    (df["Incident_type"] == "How do I")
                    & (df["ResolvedAge"] <= 7)]
                xl.to_excel(writer, sheet_name='HOW DO I - ' + str(xl.shape[0]), index=False)
                LogIt("r", "How Do I In Past 7 Days- " + str(xl.shape[0]))

                # FETCH WEEKLY RESOLVED COUNTS
                xl = df[cols][
                    (df["ResolvedWeek"] == Today.isocalendar()[1])
                    & (df["Resolved"].dt.year != 1990)
                    & (df["Assignment_Group"] == "EPOS")]
                xl.to_excel(writer, sheet_name='WEEKLY RESOLVED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Weekly Resolved - " + str(xl.shape[0]))
                xl = df[cols][
                    (df["Priority"] == 'Sev - 1')
                    & (df["ResolvedWeek"] == Today.isocalendar()[1])
                    & (df["CreatedWeek"] == Today.isocalendar()[1])]
                xl.to_excel(writer, sheet_name='WEEKLY SEV 1 - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Weekly SEV 1 - " + str(xl.shape[0]))

                # RESOLVED YESTERDAY
                xl = df["Resolved"].notnull()
                xl = df[cols][
                    (df["ResolvedAge"] >= .5)
                    & (df["ResolvedAge"] <= 1.5)
                    & (df["Assignment_Group"] == "EPOS")]
                xl.to_excel(writer, sheet_name='YDAY RESOLVED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Resolved Yesterday - " + str(xl.shape[0]))

                # OTHER COUNTS
                df["Status"].value_counts().to_excel(writer, sheet_name='Stat COUNTS')
                df["Priority"].value_counts().to_excel(writer, sheet_name='Sev COUNTS')
                df["Incident_type"].value_counts().to_excel(writer, sheet_name='INC_Type COUNTS')
            df.drop(df.index, inplace=True)
        elif inType == "P":
            # REPORT INVESTIGATIONS
            LogIt("r", "======================== INVESTIGATION SUMMARY ========================"
                  + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            cols = ['Number', 'Short_Description', 'Priority', 'Configuration_item', 'Status', 'Assigned_to',
                    'Assignment_Group', 'SLA_Status', 'CreatedAge', 'UpdatedAge', 'ClosedAge']
            with pd.ExcelWriter("C:/EPOS/REPORT/INV.xlsx") as writer:
                # WRITE THE ACTUAL DATA
                df.to_excel(writer, sheet_name='DATA', index=False)
                # FETCH NEW INV COUNTS
                xl = df[cols][
                    df["CreatedAge"] < 1]
                xl.to_excel(writer, sheet_name='NEW - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Investigations Opened Yesterday- " + str(xl.shape[0]))
                # FETCH CLOSED INV COUNTS
                xl = df[cols][
                    df["ClosedAge"] <= 1]
                xl.to_excel(writer, sheet_name='CLOSED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Investigations Closed Yesterday - " + str(xl.shape[0]))

                # FETCH WEEKLY OPENED INV COUNTS
                xl = df[cols][
                    df["CreatedWeek"] == Today.isocalendar()[1]]
                xl.to_excel(writer, sheet_name='OPENED INV W - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Investigations Opened This Week - " + str(xl.shape[0]))
                # FETCH WEEKLY CLOSED INV COUNTS
                xl = df[cols][
                    (df["ClosedAge"] <= 7)
                    & (df["ClosedWeek"] == Today.isocalendar()[1])]
                xl.to_excel(writer, sheet_name='CLOSED WEEKLY - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Investigations Closed This Week - " + str(xl.shape[0]))

                # FETCH INVESTIGATION COUNTS
                xl = df[cols][
                    (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")]
                xl.to_excel(writer, sheet_name='INV - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Workable Investigations - " + str(xl.shape[0]))
                # FETCH NOT UPDATED COUNTS
                xl = df[cols][
                    (df["Status"] != "Closed")
                    & (df["Assignment_Group"] == "EPOS")
                    & (df["UpdatedAge"] >= 10)]
                xl.to_excel(writer, sheet_name='NOT UPDATED - ' + str(xl.shape[0]), index=False)
                LogIt("r", "Investigations Not Updated In 10 Days - " + str(xl.shape[0]))
                # OTHER COUNTS
                df["Status"].value_counts().to_excel(writer, sheet_name='Stat COUNTS')
        elif inType == "ED":
            LogIt("r", "Assigned Yesterday - " + str(len(df)))
        elif inType == "EW":
            LogIt("r", "Weekly Assigned - " + str(len(df)))
            '''
            with pd.ExcelWriter("C:/EPOS/REPORT/CI.xlsx") as writer:
                xl = df.pivot_table(index='Configuration_item', values="Number", aggfunc='count')
                xl.to_excel(writer, sheet_name='Weekly CAT - ' + str(xl.shape[0]), index=False)
            '''
            xl = pd.pivot_table(df, index='Configuration_item', values="Number", aggfunc='count')
            writer = pd.ExcelWriter("C:/EPOS/REPORT/CI Cat.xlsx")
            xl.to_excel(writer, 'CI Total -' + str(xl.shape[0]))
            writer.save()
            LogIt("a", "RepData : Weekly Assigned Counted")
        else:
            LogIt("a", "RepData Error : Invalid Type")
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

# DayCheck()
if Today.isoweekday() == 1:
    eow = input("it's a Monday. Proceed with Saturday's data?").upper()
    if eow == "Y":
        Today = Today.replace(day=Today.day - 2)
    else:
        print("Proceeding with Monday's processing")

GetData(FixedPath + "incident.xlsx", "I")
GetData(FixedPath + "EPOS Assigned Daily.xls", "ED")
if Today.weekday() == 4:
    LogIt("a", "It's Friday! Weekly counting enabled.")
    GetData(FixedPath + "EPOS - Assigned  Status - Weekly (including CI).xlsx", "EW")
GetData(FixedPath + "investigation.xlsx", "P")

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
