"""

Program     :
Developer   :   Arjun Muraleedharan
Date        :   00-00-2019

Description:
This code tracks the CIP lifecycle of an item and reports anomalies.

Initial Version V1.0
This will create a DBfile based on the consolidated file created from today's run.

"""

# Libraries
import datetime
import getpass
import os
import linecache
import sys
import configparser
import pandas as pd
from functools import reduce
import shutil
import pyodbc as pdb

# Initializing variables
RunTime = datetime.datetime.now()
Tday = datetime.datetime.now().strftime("%y%m%d")
Yday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%y%m%d")

config = configparser.ConfigParser()
if config.read('TrackConfig.ini') == 0:
    config.read('TrackConfig.ini')
    FixedPath = "C:/EPOS/GETCIP/"
    FixedName = "archive.csv"
    DBFile = "CIPDB.CSV"
    LogFile = "ActivityLog.txt"
    ResultFile = "ResultLog.csv"
    ReportFile = "ReportLog.txt"
    SQLDriver = "SQL Server"
    SQLServer = "LAPTOP-ODE00V5I\SQLEXPRESS"
    SQLDatabase = "APPS_EPOS"
    SQLTable = "[APPS_EPOS].[dbo].[CIPINV]"
else:
    # config.sections()
    FixedPath = config.get('inputfiles', 'FixedPath')
    FixedName = config.get('inputfiles', 'FixedName')
    SQLDriver = config.get('inputfiles', 'SQLDriver')
    SQLServer = config.get('inputfiles', 'SQLServer')
    SQLDatabase = config.get('inputfiles', 'SQLDatabase')
    SQLTable = config.get('inputfiles', 'SQLTable')
    DBFile = config.get('outputfiles', 'DBFile')
    ResultFile = config.get('outputfiles', 'ResultFile')
    LogFile = config.get('outputfiles', 'LogFile')
    ReportFile = config.get('outputfiles', 'ReportFile')


# Query Variables
row_values = ""

ins_query = "INSERT INTO " + SQLTable + " ("
upd_query = "UPDATE " + SQLTable + " SET "
tblCols = "[UPDATEDON],[STORE],[ITEM],[CIPOT],[CIPPMR],[CIPRM],[MKDN],[PPFI],[CIPOK],[ALLOK]"


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
        os.system('pause')
    else:
        print("INVALID LOG ERROR!")
        os.system('pause')


# Prep Connection
def prepConn(dr, sr, db):
    try:
        conn = pdb.connect('Driver={'+dr+'};'
                              'Server='+sr+';'
                              'Database='+db+';'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        # prepQuery(conn, cursor, df)  # TEMPORARY DIABLE
        return conn
    except Exception as e:
        print("prepConn : " + str(e))
        os.system('pause')


def prepQuery(con, csr, df, mode='U'):
    try:
        # Split data in chunks and process each chunk. Each chunk will be updated into the DB
        _rowTotal = df.shape[0]
        _rowCount = 0
        for i, r in df.iterrows():
            _rowCount += 1
            # store = str(i[0])
            # item = str(i[1])
            updatedOn = str(datetime.datetime.now().strftime("%Y-%m-%d"))
            set_query = "[UPDATEDON] = '" + updatedOn + "',[CIPOT] = '" + r["CIPOT"] + \
                        "',[CIPPMR] = '" + r["CIPPMR"] + "',[CIPRM] = '" + r["CIPRM"] + \
                        "',[PPFI] = '" + r["PPFI"] + "',[MKDN] = '" + r["MKDN"] + "' "
            where_query = "where [STORE] = '" + str(r['STORE']) + "' and [ITEM] = '" + str(r['ITEM']) + "'"
            sql_query = upd_query + set_query + where_query
            print(sql_query)
            affectedRows = execQuery(con, csr, sql_query, _rowCount, _rowTotal)
            if affectedRows == 0:
                row_values = "','".join([str(i) for i in r.tolist()])
                row_values = updatedOn + "','" + row_values + "','" + "','"
                sql_query = ins_query + tblCols + ") VALUES ('" + row_values + "')"
                print(sql_query)
                affectedRows = execQuery(con, csr, sql_query, _rowCount, _rowTotal)
                print("Inserted " + str(affectedRows) + " rows")
            else:
                print("Updated " + str(affectedRows) + " rows")
    except Exception as e:
        print("prepQuery : " + str(e))
        os.system('pause')


def execQuery(con, csr, query, _rowCount, _rowTot):
    try:
        csr.execute(query)
        con.commit()
        if _rowCount / _rowTot == .25:
            print("execQuery : Committed 25% of the data.")
        elif _rowCount / _rowTot == .5:
            print("execQuery : Committed 50% of the data.")
        elif _rowCount / _rowTot == .75:
            print("execQuery : Committed 75% of the data.")
        else:
            pass
        return csr.rowcount
        # print("execQuery : Executed row " + str(_rows))
        # commit_count += 1
    except Exception as e:
        print("execQuery : " + str(e))
        # os.system('pause')


# First Get today's data
def GetData(db, src):
    '''
    TEMPORARY DISABLING - THE DB FILE NO LONGER NEED TO BE EXTRACTED
    '''
    LogIt("a", "GetData : Fetch data from existing DB from " + db)
    dfDBtypes = {'UPDATEDON': object, 'STORE': object, 'ITEM': object, 'CIPOT_y': object, 'CIPPMR_y': object,
                 'CIPRM_y': object, 'MKDN_y': object, 'PPFI_y': object, 'CIPOK': object, 'ALLOK': object}
    dfDBcols = ["UPDATEDON", "STORE", "ITEM", "CIPOT_y", "CIPPMR_y", "CIPRM_y", "MKDN_y", "CIPOK", "ALLOK"]
    dfDB = pd.read_csv(db, dtype=dfDBtypes)
    # dfDB.columns = ["UPDATEDON", "STORE", "ITEM", "CIPOK", "ALLOK", "CIPOT", "CIPPMR", "CIPRM", "MKDN", "PPFI"]
    srcCols = [1, 2, 3, 4, 5, 6]
    srcDtypes = {'1': object, '2': object, '3': object, '4': object, '5': object, '6': object}
    LogIt("a", "GetData : Get data from today's file " + src)
    df_tmp = pd.read_csv(src, usecols=srcCols, dtype=srcDtypes)
    CleanData(dfDB, df_tmp)  # Commenting as the above section will be removed
    #  CleanData(df_tmp)


# Clean the data and get make it useful
# def CleanData(dfDB, df_tmp):
def CleanData(dfDB, df_tmp):
    # df_tmp = df_tmp.fillna(0)
    '''
    TEMPORARY REMOVAL AS THIS DF IS NOT PASSED OVER
    '''
    dfDB.set_index(['STORE', 'ITEM'], inplace=True)
    dfDB.sort_index(inplace=True)  # Sort based on store & item numbers
    LogIt("a", "CleanData: Cleaned dfDB")

    # Process today's CIPOT records
    LogIt("a", "CleanData: Cleaning df_CIPOT")
    CIPOTCols = ["STORE", "ITEM", "CIPOT"]
    df_CIPOT = df_tmp[['2', '4', '3']][(df_tmp['1'] == 'POT') & (df_tmp['3'] != 'H') & (
                df_tmp['3'] != 'T')]  # Select required columns only, arranged as required
    df_CIPOT.columns = CIPOTCols  # Set column names
    # df_CIPOT = df_CIPOT[["DATE", "FILETYPE", "STORE", "ITEM", "REC_TYPE"]]  # re-arrange column names
    df_CIPOT["STORE"] = df_CIPOT["STORE"].str[-4:].astype(int)  # Extract store number & cast to int
    df_CIPOT["ITEM"] = df_CIPOT["ITEM"].astype(int)
    df_CIPOT["CIPOT"] = df_CIPOT["CIPOT"].astype(str)
    df_CIPOT["CIPOT"].fillna('X', inplace=True)
    df_CIPOT = df_CIPOT.set_index(['STORE', 'ITEM'])  # Setting new index for data comparison
    df_CIPOT.sort_index(inplace=True)

    # Process CIPPMRs
    LogIt("a", "CleanData: Cleaning df_CIPPMR")
    CIPPMRCols = ["STORE", "ITEM", "CIPPMR"]
    df_CIPPMR = df_tmp[['2', '4', '3']][df_tmp['1'] == 'PMR']
    df_CIPPMR.columns = CIPPMRCols
    df_CIPPMR["STORE"] = df_CIPPMR["STORE"].str[-4:].astype(int)
    df_CIPPMR["ITEM"] = df_CIPPMR["ITEM"].astype(int)
    df_CIPPMR["CIPPMR"] = df_CIPPMR["CIPPMR"].astype(str)
    df_CIPPMR["CIPPMR"].fillna('X', inplace=True)
    df_CIPPMR = df_CIPPMR.set_index(['STORE', 'ITEM'])
    df_CIPPMR.sort_index(inplace=True)

    # Process CIPRMs
    LogIt("a", "CleanData: Cleaning df_CIPRM")
    CIPRMCols = ["STORE", "ITEM", "CIPRM"]
    df_CIPRM = df_tmp[['2', '5', '4']][(df_tmp['1'] == 'PRM') &
                                       (df_tmp['3'] == Yday)]  # This is to get latest CIPRM records
    df_CIPRM.columns = CIPRMCols
    df_CIPRM["STORE"] = df_CIPRM["STORE"].str[-4:].astype(int)
    df_CIPRM["ITEM"] = df_CIPRM["ITEM"].astype(int)
    df_CIPRM["CIPRM"] = df_CIPRM["CIPRM"].astype(str)
    df_CIPRM["CIPRM"].fillna('X', inplace=True)
    df_CIPRM = df_CIPRM.set_index(['STORE', 'ITEM'])
    df_CIPRM.sort_index(inplace=True)

    # Process PPFI
    LogIt("a", "CleanData: Cleaning df_PPFI")
    PPFICols = ["STORE", "ITEM", "PPFI"]
    df_PPFI = df_tmp[['2', '3', '6']][(df_tmp['1'] == 'PFI') &
                                      (df_tmp['4'] == 'R')]  # This is to filter out trailer records
    df_PPFI.columns = PPFICols
    df_PPFI["STORE"] = df_PPFI["STORE"].str[-4:].astype(int)
    df_PPFI["ITEM"] = df_PPFI["ITEM"].astype(int)
    df_PPFI["PPFI"] = df_PPFI["PPFI"].astype(str)
    # df_PPFI["PPFI"].fillna('X', inplace=True)
    df_PPFI = df_PPFI.set_index(['STORE', 'ITEM'])
    df_PPFI.sort_index(inplace=True)

    # Process Markdown Flag
    LogIt("a", "CleanData: Cleaning df_CIPLOG")
    LOGCols = ["STORE", "ITEM", "MKDN"]
    df_CIPLOG = df_tmp[['2', '3', '4']][
        (df_tmp['1'] == 'LOG')]  # Select required columns only, arranged as required
    df_CIPLOG.columns = LOGCols  # Set column names
    df_CIPLOG["STORE"] = df_CIPLOG["STORE"].str[-4:].astype(int)  # Extract store number & cast to int
    df_CIPLOG["ITEM"] = df_CIPLOG["ITEM"].astype(int)
    df_CIPLOG["MKDN"] = 'Y'  # This is to indicate item is on CIP
    df_CIPLOG = df_CIPLOG.set_index(['STORE', 'ITEM'])
    df_CIPLOG.sort_index(inplace=True)

    '''
    # TEMPORARY DISABLED
    # Now call Update Function
    UpdDFDB(dfDB, df_CIPLOG, 'MKDN')
    UpdDFDB(dfDB, df_CIPOT, 'CIPOT')
    UpdDFDB(dfDB, df_CIPPMR, 'CIPPMR')
    UpdDFDB(dfDB, df_CIPRM, 'CIPRM')
    dfDB.sort_index(inplace=True)
    dfDB.reset_index(inplace=True)
    '''

    # The disabled function is done below
    # dfs = [dfDB, df_CIPOT, df_CIPPMR, df_CIPRM, df_CIPLOG, df_PPFI]  # COMMENTING AS THE DF IS NOT LONGER USED
    dfs = [df_CIPOT, df_CIPPMR, df_CIPRM, df_CIPLOG, df_PPFI]
    df_Tday = reduce(lambda left, right: pd.merge(left, right, on=['STORE', 'ITEM'], how='outer'), dfs)
    # df_Tday = pd.concat(dfs, join='outer', axis=1)

    # Now sort & reset index to retain the original columns
    df_Tday.sort_index(inplace=True)  # Sort based on store & item numbers
    # df_Tday.reset_index(inplace=True)  # Reset index to retrieve the store and item columns
    df_Tday["CIPOT"] = df_Tday["CIPOT"].fillna('X')
    df_Tday["CIPPMR"] = df_Tday["CIPPMR"].fillna('X')
    df_Tday["CIPRM"] = df_Tday["CIPRM"].fillna('X')
    df_Tday["MKDN"] = df_Tday["MKDN"].fillna('X')
    df_Tday = df_Tday.fillna('X')

    # Pass the data frame & Columns to save to DB
    # prepConn(SQLDriver, SQLServer, SQLDatabase, df_Tday)  # DISABLE THIS FOR TESTING
    # UpdateDB(df_Tday)
    CheckCipDB(dfDB, df_Tday)  # Compare data in 2 tables
    # RepCipDB(df_Tday)
    # RepCipDB(df_Tday, dfDB.columns)
    # RepCipDB(pd.concat([dfDB, df_CIPOT], ignore_index=True))
    # CheckCipDB(df_CIPOT, df_CIPPMR, df_CIPRM)


# Now update the CIPOT, CIPPMR, CIPRM, GETCIP flags of each item on their stores
def UpdateDB(df):
    LogIt("a", "UpdDFDB: Updating DF to DB")
    for i, r in df.iterrows():
        store = str(i[0])
        item = str(i[1])
        updatedOn = str(datetime.datetime.now().strftime("%Y-%m-%d"))
        set_query = '[UPDATEDON] = ' + updatedOn + \
                    ',[CIPOT] = ' + r['CIPOT'] + ',[CIPPMR] = ' + r['CIPPMR'] + ',[CIPRM] = ' + r['CIPRM'] + \
                    ',[PPFI] = ' + r['PPFI'] + ',[MKDN] = ' + r['MKDN'] + ',[CIPOK] = ' + r['CIPOK'] + ' '
        where_query = 'where [STORE] = ' + store + ' and [ITEM] = ' + item + ','
        print(upd_query + set_query + where_query)
        '''
        try:
            dfMaster.loc[i, "DATERUN"] = r["DATERUN"]
            dfMaster.loc[i, col] = r[col]
            LogIt("a", "Store " + store + " processed item " + item + " to " + col)
        except KeyError as e:
            LogIt("a", "Store " + store + " failed Key processing item " + item + " to " + col)
        except ValueError as e:
            LogIt("a", "Store " + store + " failed Value processing item " + item + " to " + col)
        '''
    LogIt("a", "UpdDFDB: Updated DB")


# Check the process flow
def CheckCipDB(dfDB, df_Tday):
    try:
        LogIt("a", "CheckCipDB : Comparing Today's data with master DB")
        dfNew = pd.DataFrame(columns=['STORE', 'ITEM', 'CIPOT', 'CIPPMR', 'CIPRM', 'MKDN', 'PPFI', 'UPDATEDON'])
        # dfNew.set_index(['STORE', 'ITEM'], inplace=True)
        dfUpd = pd.DataFrame(columns=['STORE', 'ITEM', 'CIPOT', 'CIPPMR', 'CIPRM', 'MKDN', 'PPFI', 'UPDATEDON'])
        dfUpd.set_index(['STORE', 'ITEM'], inplace=True)
        _rowTot = df_Tday.shape[0]
        _rowCount = 0
        '''
        for i, row in dfDB.iterrows():  # TESTING PURPOSE
            print(i, row.tolist())
        '''
        for i, row in df_Tday.iterrows():
            # print(i, row.tolist())
            if i in dfDB.index:  # True if the store, item from Today's data is in DB
                print(dfDB.iloc[i])
                changed_row = {'STORE': str(i[0]), 'ITEM': str(i[1])}  # to get the store and item number from the index
                changed_row.update(dict(row))  # this will add the row values as a dictionary
                dfDB.update(changed_row, ignore_index=True)
                dfDB.iloc[i, 'UPDATEDON'] = str(datetime.datetime.now().strftime("%Y-%m-%d"))
                # print("FOUND")
            else:
                new_row = {'STORE': str(i[0]), 'ITEM': str(i[1])}  # to get the store and item number from the index
                new_row.update(dict(row))  # this will add the row values as a dictionary
                dfNew = dfNew.append(new_row, ignore_index=True)
                # dfNew = dfNew.update(pd.DataFrame(dict(row), index=i))
            # dfNew.update(pd.DataFrame(dict(row), index=i))

            # _rowDict = {'DATERUN': row[0], 'STORE': row[1], 'ITEM': row[2], 'CIPPMR': row[3]}
            # dfDB.update(pd.DataFrame(_rowDict, index=[len(dfDB)+1]))
            _rowCount += 1
            if _rowCount / _rowTot == .25:
                print("CheckCipDB : Committed 25% of the data.")
            elif _rowCount / _rowTot == .5:
                print("CheckCipDB : Committed 50% of the data.")
            elif _rowCount / _rowTot == .75:
                print("CheckCipDB : Committed 75% of the data.")
            else:
                pass
        dfNew["UPDATEDON"] = str(datetime.datetime.now().strftime("%Y-%m-%d"))
        LogIt("a", str(dfNew.shape))
        dfNew.to_csv(FixedPath+'New.csv', index=False)
        LogIt("a", "CheckCipDB : Completed checks in DB " + FixedPath + DBFile)
    except Exception as e:
        LogIt("e", str(e))

    # df1 = df_CIPOT.sort_index()
    # df2 = df_CIPPMR.sort_index()

    '''
    for POTi, POTr in df_CIPOT.iterrows():  # Update CIPPMR for items in CIPOT
        store = str(POTi[0])
        item = str(POTi[1])
        # dfDB.iloc[POTi] = POTr.tolist()
        try:
            # print(i, row['CIPOT'])
            # LogIt("a", df_CIPPMR.loc[POTi])
            dfDB.loc[POTi, "CIPPMR"] = df_CIPPMR.loc[POTi, "CIPPMR"]  # Pick CIPPMR flag of CIPOT store,item
            # LogIt("a", df_CIPPMR.loc[POTi, "CIPPMR"])
            LogIt("a", "Store " + store + " successfully processed item " + item + " to CIPPMR")
        except KeyError as e:
            LogIt("r", "Store " + store + " not processed item " + item + " to CIPPMR")
        except ValueError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPPMR")
        try:
            # print(i, row['CIPOT'])
            dfDB.loc[POTi, "CIPRM"] = df_CIPRM.loc[POTi, "CIPRM"]
            LogIt("a", "Store " + str(POTi[0]) + " successfully processed item " + str(POTi[1]) + " to CIPRM")
        except KeyError as e:
            LogIt("r", "Store " + str(POTi[0]) + " not processed item " + str(POTi[1]) + " to CIPRM")
        except ValueError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPRM")


    for DBi, DBr in dfDB.iterrows():
        store = str(DBi[0])
        item = str(DBi[1])
        try:
            # print(i, row['CIPOT'])
            # LogIt("a", df_CIPPMR.loc[POTi])
            dfDB.loc[DBi, "CIPPMR"] = df_CIPPMR.loc[DBi, "CIPPMR"]  # Pick CIPPMR flag of new CIPPMR store,item
            # LogIt("a", df_CIPPMR.loc[POTi, "CIPPMR"])
            LogIt("a", "Store " + store + " successfully updated item " + item + " to CIPPMR")
        except KeyError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPPMR")
        except ValueError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPPMR")
        try:
            # print(i, row['CIPOT'])
            dfDB.loc[DBi, "CIPRM"] = df_CIPRM.loc[DBi, "CIPRM"]  # Pick CIPRM flag of new CIPRM store,item
            LogIt("a", "Store " + store + " successfully updated item " + item + " to CIPRM")
        except KeyError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPRM")
        except ValueError as e:
            LogIt("r", "Store " + store + " not updated item " + item + " to CIPRM")

    '''
    # RepCipDB(dfDB.reset_index())
    # RepCipDB(dfDB)


# Report Anomalies
def RepCipDB(df):
    LogIt("a", "RepCipDB : Writing updated data to DB")
    df.reset_index(inplace=True)
    # shutil.copyfile(FixedPath + DBFile, FixedPath + 'CIPDB_BAK.csv')
    df["UPDATEDON"] = str(datetime.datetime.now().strftime("%Y-%m-%d"))
    _dfCols = df.columns
    cols = [col for col in _dfCols if '_x' not in col]
    # df["UPDATEDON"] = str(datetime.datetime.now().strftime("%Y-%m-%d"))
    # cols = ["STORE", "ITEM", "CIPOT", "CIPPMR", "CIPRM", "MKDN", "CIPOK", "ALLOK"]
    df[cols].to_csv(FixedPath+DBFile, index=False)
    LogIt("a", "RepCipDB : Saved" + str(df.shape) + " data to : " + FixedPath + DBFile)


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
        # dfDB = pd.read_csv(FixedPath+DBFile).set_index(['STORE', 'ITEM'])  # Fetch DB & set new index for data comparison
        GetData(FixedPath + DBFile, FixedPath + FixedName)
        # GetCipDB(FixedPath + DBFile)

        # Log Footer
        LogIt("r", "------------------------------------------------------------------------------------------------")
        LogIt("r", "Completed at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        LogIt("r", "------------------------------------------------------------------------------------------------")

        # Log program runtime
        RunTime = datetime.datetime.now() - RunTime
        LogIt("a", "Program completed successfully in " + str(RunTime))

# Wait user input for exit
os.system('pause')
