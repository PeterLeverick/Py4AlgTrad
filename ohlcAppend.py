""" 
    Author: Peter Leverick Jan 2023 

    Historical data
    -Kraken only provides the last 720 data points
    -Script 1 --> To get larger database, we transform ticks into OHLC (see ticksToOhlc.py) to create the MasterFile
                    Tgis is a vry long process (~12hours) than we do not want to run on a regular basis
    -Script 2 --> Once we have tha MasterFile, we want to append new candles as we go (this script)
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

#--- get master ohlc from csv 
def get_master_ohlc_CSV():
    #datafile = '.data/ohlc2021.csv'
    path_csv = './data/'
    name_csv = 'master_ohlc.csv'
    OHLC_csv = path_csv + name_csv
    asset_df = pd.read_csv(OHLC_csv, index_col = 'Date')

    # Converting the dates from string to datetime format:
    asset_df.index = pd.to_datetime(asset_df.index)
    asset_df[['Open', 'High', 'Low', 'Close']] = asset_df[['Open', 'High', 'Low', 'Close']].apply(pd.to_numeric)
    #asset_df = asset_df.iloc[:-1,:]            # no needed, in the new lib version last row is valid as is the last day  
    
    # this is from master_ohlc.csv, don't get confused is not the last OHLC
    print (f"\n master_ohl.csv second last date --> {asset_df.index[-2]}  close price --> {asset_df['Close'].iloc[-2]}")   
    print (f"       master_ohlc.csv last date --> {asset_df.index[-1]}  close price --> {asset_df['Close'].iloc[-1]}")   

    master_last_date = asset_df.index[-2]      # this is for since later to get OHLC

    return asset_df, master_last_date


''' get ohlc from kraken '''
def get_ohlc(crypto_pair='ETHUSDT', interval=1440, since=0):

    asset_df = kraken_libs.kraken_ohlc_lib.main(crypto_pair, interval, since)

    print(asset_df)
    what_column = ['Close']
    #print (f"\n last date {asset_df.index[-1]}  {asset_df[what_column].iloc[-1]}")      
    #print (f"\n len df {len(asset_df)}")
    return asset_df


''' append  new_ohlc to master_ohlc '''
def append_new_ohlc_to_master(master_ohlc_df, new_ohlc_df):

    print (f"\nlen master_ohlc --> {len(master_ohlc_df)}")
    print (f"len new_ohlc --> {len(new_ohlc_df)}")
    master_ohlc_df = pd.concat([master_ohlc_df, new_ohlc_df], axis="rows", join="outer")
    print(master_ohlc_df.head())
    print(master_ohlc_df.tail())
    print (f"len new master_ohlc --> {len(master_ohlc_df)}")

    return master_ohlc_df


''' export new master_ohlc to csv '''
def export_new_master_ohlc(master_ohlc_df):

    #we might want to export to a different file to keep a copy of the original version 
    path_csv = './data/'
    name_csv = 'master_ohlc_jan22_2022.csv'
    OHLC_csv = path_csv + name_csv

    master_ohlc_df.to_csv(OHLC_csv) 

    return 




'''-----------------------------------------------------------------'''
'''                 Main Function                                   '''
'''-----------------------------------------------------------------'''
def main():

    ''' get master ohlc from csv '''
    master_ohlc_df, since = get_master_ohlc_CSV()
    #g = input("get OHLC from Master file  .... Press any key : ")


    ''' get ohlc from kraken '''
    since = datetime.timestamp(since)    # old since = str  #since = str(since)   #since = since[0:10] + '0'* 9
    since=int(since)
    crypto_pair = 'ETHUSDT'
    interval = 1440
    new_ohlc_df = get_ohlc(crypto_pair, interval, since)
    #g = input("get OHLC from API .... Press any key : ")


    ''' append new_ohlc to master df '''
    master_ohlc_df = append_new_ohlc_to_master(master_ohlc_df, new_ohlc_df)
    #g = input("append new_ohlc to master df  .... Press any key : ")


    ''' export new master_ohlc to csv '''
    export_new_master_ohlc(master_ohlc_df)
    #g = input("export new master_ohlc to csv  .... Press any key : ")


    return



  
if __name__== "__main__":
  main()
  g = input("End Program  .... Press any key : "); print (g)



