import sys
import time
import pickle
import glob
import piutil
import clr
import PIconnect
sys.path.append(r'C:\Program Files (x86)\PIPC\AF\PublicAssemblies\4.0')    
clr.AddReference('OSIsoft.AFSDK')  
from OSIsoft.AF import *  
from OSIsoft.AF.PI import *  
from OSIsoft.AF.Asset import *  
from OSIsoft.AF.Data import *  
from OSIsoft.AF.Time import *  
import pyodbc
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

def PM(pm) :
    
    if pm in ['PM1', 'PM17', 'PM3', 'PM16'] :

        pi_server = 'SKIC-PISERVER.cementhai.com'  # '172.31.224.78'
        
    elif pm in ['PM4', 'PM5', 'PM6', 'PM7', 'PM8', 'PM9'] :

        pi_server = 'SKICWSPIDB01.cementhai.com' #172.29.8.221
        
    return pi_server
    
afServers = PISystems()  
afServer = afServers.DefaultPISystem
DB = afServer.Databases.DefaultDatabase
pi_server = 'SKIC-PISERVER.cementhai.com'
piServers = PIServers()    
piServer = piServers.DefaultPIServer

def PI_interpolate_value(server, tag, start_time, end_time, span_time) :
    #Read archive with interpolated value from PI
    start_time_1 = start_time
    end_time_1 = end_time
    piServer = PIServer.FindPIServer(server) 
    df = pd.DataFrame()
    timerange = AFTimeRange(start_time_1, end_time_1)
    span = AFTimeSpan.Parse(span_time)
    pt = PIPoint.FindPIPoint(piServer, tag)
    interpolated = pt.InterpolatedValues(timerange, span, "", False)
    for event in interpolated :
        if str(event.Value)  in  ['[-11059] No Good Data For Calculation', 'Bad','I/O Timeout','Unit Down','Bad Input','Scan Timeout','No Data','Over Range','Intf Shut','Input','Tag not found']:
            df.loc[str(event.Timestamp.LocalTime),'Value'] = None           
        else:
            df.loc[str(event.Timestamp.LocalTime),'Value'] = event.Value     
        df.index.name = 'Time'
    #df.index = pd.to_datetime(df.index)
    df = df.sort_index(ascending=True)
    return df

def write_to_PI(server, df_path, df, string_) :

    piServer = PIServer.FindPIServer(server) 
    AFval = AFValue()
    #piServer = piServers.DefaultPIServer;  #Pending connect by IP
    #print(piServer)
    timestamp = datetime.now() + pd.offsets.DateOffset(years = 0)
    #print(timestamp)
    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')#+ pd.offsets.DateOffset(years=543) #relativedelta(years=543)
    #print(timestamp)
    for index, row in df.iterrows():

        #print(index)
        #tag = df_path.loc[str(index),'Value']
        tag = df_path.loc[index,'Value']
        #print(tag)
        value = str(row['Value'])
        #print(value)
        writept = PIPoint.FindPIPoint(piServer, tag)  
        AFval.Value = value
        AFval.Timestamp = AFTime(timestamp)
        writept.UpdateValue(AFval, AFUpdateOption.Replace, AFBufferOption.BufferIfPossible)

    print("{}    {}  : {}".format(timestamp, string_, value))
    
def Validation(df, path_, string_) :

    if df.isna().sum().sum() == 0 :
        
        value = df
        value = pd.DataFrame(value, columns=['Value'])
        path = path_
        path = pd.Series(path)
        path = pd.DataFrame(path,columns=['Value'])   
        write_to_PI(pi_server, path, value, string_)
        
    else :
        
        timestamp = datetime.now() + pd.offsets.DateOffset(years = 0)
        timestamp = timestamp.strftime('%Y-%m-%d %H:%M')
        print("{}    {}  : ERROR".format(timestamp,string_))
