from datetime import datetime , timedelta
import itertools
import pandas as pd
import numpy as np
import collections
import time
import math
import pyodbc

def upload_command_line(col, table) :

    column = ' (' + col + ') '
    q = ' (?)'
    
    return "INSERT INTO " + table + column + "VALUES" + q

def update_command_line(col, table, value, lastid) : 
    
    return 'UPDATE ' + table + ' SET ' + col + ' = \'' + str(value) + '\' WHERE id = ' + lastid

def PM_database(PM) :
    
    if PM in ['PM1', 'PM17', 'PM3', 'PM16']:
    
        server = '172.31.224.57\skicqa'
        database = 'AL300'
        username = 'AL300'
        password = 'al300'
        pi_server = 'SKIC-PISERVER.cementhai.com' # '172.31.224.78'

    elif PM in ['PM4', 'PM5', 'PM6', 'PM7', 'PM8', 'PM9']:
    
        server = '172.29.8.155'
        database = 'AL300WS'
        username = 'AL300'
        password = 'al300'
        pi_server = 'SKICWSPIDB01.cementhai.com' #172.29.8.221
        
    return server, database, username, password, pi_server

def upload_database(dataframe, table, server = '10.28.59.29', database = 'DevApp',
                    username = 'UTA_Owner', password = 'P@ssw0rd#2020') :

    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';DATABASE=' + database + 
                          ';UID=' + username + ';PWD=' + password)
    
    i = 0
    while (i < len(list(dataframe.columns))) :
        
        ### Get column name
        col = list(dataframe.columns)[i]
        if (i == 0) :
            
            cursor = conn.cursor()
            
            ### Upload first col value
            cursor.execute(upload_command_line(col, table), dataframe[col][0])
            
            ### Get last upload id
            cursor.execute("SELECT @@IDENTITY AS ID;")
            lastid = cursor.fetchone()[0]
            conn.commit()
            
        else :
            
            ### Update the rest to the same id column
            cursor = conn.cursor()
            print(update_command_line(col, table, dataframe[col][0], str(lastid)))
            cursor.execute(update_command_line(col, table, dataframe[col][0], str(lastid)))
            conn.commit()   
            
        i+=1
    
def download_database(Properties, table, server = '10.28.59.29', database = 'DevApp',
                      username = 'UTA_Owner', password = 'P@ssw0rd#2020') :
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';DATABASE=' + database + 
                          ';UID=' + username + ';PWD=' + password)
    cursor = conn.cursor()
    #print(cnxn)
    tsql = "SELECT " + str(Properties) + " FROM " + table + ";"
    df = pd.read_sql(tsql,conn)
    
    return df

def update_database(table, tag_value, value, tag_condition, condition, server = '10.28.59.29', 
                    database = 'DevApp', username = 'UTA_Owner', password = 'P@ssw0rd#2020') :

    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';DATABASE=' + database + 
                          ';UID=' + username + ';PWD=' + password)
    cursor = conn.cursor()
    cursor.execute('UPDATE '+ table + ' SET ' + tag_value + ' = \'' + str(value) + '\' WHERE ' + tag_condition + ' = ' + str(condition))
    conn.commit()