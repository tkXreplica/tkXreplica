from pi import * 
from datetime import datetime, timedelta

def pi_tag_table(pm, taglist, lag_type, start_lag, end_lag):

    now = datetime.now()
    if lag_type == 'minutes':
        
        start = now - timedelta(minutes = start_lag)
        end = now - timedelta(minutes = end_lag)
        
    elif lag_type == 'hours':
        
        start = now - timedelta(hours = start_lag)
        end = now - timedelta(hours = end_lag)
        
    elif lag_type == 'days':
        
        start = now - timedelta(days = start_lag)
        end = now - timedelta(days = end_lag)
    
    #select start date - end date
    pi_server = PM(pm)
    StartTime = datetime.strftime(datetime(start.year, start.month, start.day, start.hour, start.minute, 0), "%Y-%m-%d %H:%M:00")
    EndTime = datetime.strftime(datetime(end.year, end.month, end.day, end.hour, end.minute, 0), "%Y-%m-%d %H:%M:00")
    
    avgtime = '1m'
    dfP = pd.DataFrame()
    dfPI = pd.DataFrame()

    itag = 0
    while itag < len(taglist):
        
        dfPI = PI_interpolate_value(pi_server, taglist[itag], str(StartTime), str(EndTime), str(avgtime))
        dfPI.rename(columns={'Value' : str(taglist[itag])}, inplace=True) 
        dfP = pd.concat([dfP, dfPI], axis = 1)
        itag = itag + 1
        
    return dfP

def vader(pm, tag) :

    server, database, username, password, pi_server = PM(pm)
    with PIconnect.PIServer(server = pi_server) as piserver :
        
        df_tag = tag
        Value = piserver.search(df_tag)
        
    if isinstance(Value,str) == True :
        
        Value = 0
        
    data = str(eval('Value[0].interpolated_values("*","*","1m").iloc[-1]'))
    return data