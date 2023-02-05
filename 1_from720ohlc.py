""" 
    Author: Peter Leverick Jan 2023 

    Historical data
    -Kraken only provides the last 720 data points
    -Script 1 (this one) --> Simply to show where does the last 720 OHLC starts from with regards to closen OHLC time-window
                            This is only an input for Script 2
    -Script 2 --> To get larger database, we transform ticks into OHLC (see ticksToOhlc.py) to create the MasterFile
                    This is a very long process (up to ~12hours) than we do not want to run on a regular basis
    -Script 3 --> Once we have tha MasterFile, we want to append new candles as we go (this script)
                    1. Open MasterFile.csv
                    2. Get OHLC from kraken (<= 720 datapoints)
                    3. Concat the two df
                    4. create the new master file (up to date)
"""

import pandas as pd
import numpy as np
from numpy.core.fromnumeric import round_

import datetime
from datetime import date
from datetime import datetime, timedelta
import time, requests, sys, os
from time import ctime

from requests.exceptions import HTTPError

import matplotlib.pyplot as plt
pd.plotting.register_matplotlib_converters()
#%matplotlib inline   # This is needed if you're using Jupyter to visualize charts:
import mplfinance as mpf
import csv


#------- import libraries 
import kraken_libs


#----------- funtions 

''' get ohlc from kraken '''
def get_ohlc(crypto_pair='ETHUSDT', interval=1440, since=0):

    asset_df = kraken_libs.kraken_ohlc_lib.main(crypto_pair, interval, since)

    print(asset_df.head())
    print(asset_df.tail())

    return asset_df


'''-----------------------------------------------------------------'''
'''                 Main Function                                   '''
'''-----------------------------------------------------------------'''
def main():
    
    ''' get ohlc from kraken '''
    '''   we want to see the first interval --> it will be a manual input for 2_ticksToOhlc.py'''
    since = 0    # old since = str  #since = str(since)   #since = since[0:10] + '0'* 9
    since=int(since)
    crypto_pair = 'ETHUSDT'
    interval = 1440
    new_ohlc_df = get_ohlc(crypto_pair, interval, since)
    #g = input("get OHLC from API .... Press any key : ")


    return



  
if __name__== "__main__":
  main()
  g = input("End Program  .... Press any key : "); print (g)



