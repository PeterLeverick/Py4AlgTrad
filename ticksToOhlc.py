"""

    OHLC --> Ticks storaged in a list before transforming into OHLC (much faster than dataframe)  
    
        1 month 950k ticks = 30min    ---  2019 jan-dec = len (l) -> 6.4M records ~10 to 12h
    
"""



#------------------------------------------------------------------------
#  
#           OHLC from Ticks (Trades)
#
# source: 
#  https://www.kraken.com/features/api#get-recent-trades
#
#  How to retrieve historical time and sales (trading history) using the REST API Trades endpoint.
#  https://support.kraken.com/hc/en-us/articles/218198197-How-to-retrieve-historical
#
#   url = 'https://api.kraken.com/0/public/OHLC'  #only 720
#   url = 'https://api.kraken.com/0/public/Trades' 
#
# https://api.kraken.com/0/public/Trades?pair=xbtusd&30&since=1559347200000000000
#
#------------------------------------------------------------------------

"""
#------------------------------------------------------------------------
Get recent trades
URL: https://api.kraken.com/0/public/Trades

Input:

pair = asset pair to get trade data for
since = return trade data since given id (optional.  exclusive)
Result: array of pair name and recent trade data

<pair_name> = pair name
    array of array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>, <miscellaneous>)
last = id to be used as since when polling for new trade data

The Trades endpoint takes an optional parameter named since, which specifies the starting date/time of the data. 
The since value is a UNIX timestamp at nanosecond resolution (a standard UNIX timestamp in seconds with 
9 additional digits).

For example, a call to the Trades endpoint such as 
https://api.kraken.com/0/public/Trades?pair=xbtusd&since=1559347200000000000 
would return the historical time and sales for XBT/USD from the 1st of June 2019 at 00:00:00 UTC:
#------------------------------------------------------------------------
"""

import pandas as pd 
import numpy as np
import datetime
import matplotlib.pyplot as plt
import time, json, requests, sys
from time import time, ctime
import time
from datetime import datetime
from requests.exceptions import HTTPError

# Check pandas version
print(f"\n Pandas version --> {pd.__version__}\n")
#print(pd.__version__)

#url kraken 
url = 'https://api.kraken.com/0/public/Trades' 

## 1st since for kraken 
from datetime import datetime
since = datetime(2019, 12, 31, 19, 00, 00000)  
since = datetime.timestamp(since)
print("timestamp =", since)
since = str(since)
since = since[0:10] + '0'* 9            ## should it be integer in new api version ??
print(since)

#my_origin for resampling 
myorigin='2019-12-31 19:00:00'  # string should be equal to since and now 

# Dates while loop   
now = datetime(2019, 12, 31, 19, 00, 00000)
print(now)
now = int(datetime.timestamp(now))
print(now)

end = datetime(2021, 2, 3, 19, 00, 00000) #Eastern
print(end)
end = int(datetime.timestamp(end))
print(end)
g = input("dates range : ") 

# time control scrip execution
time_start = ctime(int(time.time()))
time_end = ctime(int(time.time()))

# df to compute OHLC
df = pd.DataFrame(columns=['Date', 'Price','Volume', 'BS'])

# list rolling ticks from kraken (much faster than a df)
l = []
l_labels = ['Date', 'Price','Volume', 'BS']

# CSV with OHLC
path_csv = './data/'
name_csv = 'master_ohlc.csv'
OHLC_csv = path_csv + name_csv

# CSV with ticks
t_path_csv = './data/'
t_name_csv = 'master_ticks.csv'
ticks_csv = t_path_csv + t_name_csv
ticks_csv

# 0 get ticks within the timeframe from API and store them into a list 
while now <= end:
    
    print('time --> {}    date --> {}'.format(now, ctime(now)))
    
    #time.sleep(1.75)
    time.sleep(2)

    for i in range(3):
        try:
            response_kraken = requests.post(url, 
                                            params=
                                            {'pair':'ETHUSDT',
                                             'since':since},
                                             headers={"content-type":"application/json"}
                                             )
            # If the response was successful, no Exception will be raised
            response_kraken.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error occurred: {err}')  # Python 3.6
        else:
            print('Success, URL found!')
            break
        print('--------------------  try #');print(i)
        time.sleep(8)

    if i == 2: sys.exit('URL error GGC')     

    response_json = response_kraken.json()  
    #print(response_json)                    

    since = response_json['result']['last']

    i = 0
    while True:

        try:
            new_row = [ctime(response_json['result']['ETHUSDT'][i][2]),
                       response_json['result']['ETHUSDT'][i][0],
                       response_json['result']['ETHUSDT'][i][1],
                       response_json['result']['ETHUSDT'][i][3]
                       ] 
            if int((response_json['result']['ETHUSDT'][i][2])) <= end: print (new_row)
            if int((response_json['result']['ETHUSDT'][i][2])) <= end: l += [new_row]
            i += 1
            now = int((response_json['result']['ETHUSDT'][i][2]))
            
        except Exception as err:
            print(f'end while: {err}')  # Python 3.6
            break 

# 1 ' tranform list into a DataFrame df and work out  OHLC .... '
print (' tranform list into a DataFrame df and work out  OHLC .... ')
df = pd.DataFrame.from_records(l, columns=l_labels)   # l --> list, l_labels --> column names 
df['Date'] = pd.to_datetime(df['Date'])
df.set_index('Date', inplace=True) 

# 2 resampling 
# important to set origin (day:hour:min:sec origin), 1st tick might be delayed, so days close lagging 
# origin works with pandas version 1.3.5 
# label='right', closed='right' --> from 7pm to 7pm is tomorrow's close (take the label/close at the end not the begining) 
# data_OHLC = pd.to_numeric(df['Price']).resample('1440min', origin='2022-12-31 19:00:00').ohlc()

# 2.1 OHLC
timeframe = 'D'    # '1440min'   #timeframe = 'D'    #timeframe = '15Min'
print (f"\nresampling timeframe --> {timeframe}")
print (f"\nresampling timestamp origin --> {timeframe}")
data_OHLC = pd.to_numeric(df['Price']).resample(timeframe, origin=myorigin, label='right', closed='right').ohlc()
data_OHLC.columns = ["Open", "High", "Low", "Close"]    # change columns names
print (data_OHLC.head())

# 2.2 Volume
volume_OHLC = pd.to_numeric(df['Volume']).resample(timeframe, origin=myorigin, label='right', closed='right').sum()
volume_OHLC = pd.DataFrame(volume_OHLC, columns=['Volume'])     #transform in df, date becomes index
print (volume_OHLC.head())

# 3 concat OHLC + Volume 
data_OHLC = pd.concat([data_OHLC, volume_OHLC], axis="columns", join="outer")
print (data_OHLC.head())
print (data_OHLC.tail())

# 4 export OHLC + Volume to csv
data_OHLC.to_csv(OHLC_csv)

# 5 export ticks to csv
l[1:10]
l[-10:-1]
df.to_csv(ticks_csv)

# this whole script --> how long does it last?
print(); print()
print (' TimeStart --->  {}'.format(time_start))
time_end = ctime(int(time.time()))
print (' TimeEnd --->    {}'.format(time_end))
print()
print (' len l --->      {}'.format(len(l)))


g = input("end of script : ") 


