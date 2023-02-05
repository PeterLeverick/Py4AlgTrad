'''
         V2 Created on Saturday January 21st 2023
1/ This version fixes the lag between OHLC (e.g. close) and date 
The issue was --> prices lagging on day --> OHLC lib vs Kraken browser 

2/ this version fixes data ryoe for Kraken API OHLC --> interval=integer, since=integer 
Get OHLC Data --> https://docs.kraken.com/rest/#tag/Market-Data/operation/getTickerInformation

!!!! need to review this library for other porgrams that uses it ???

@author: peter leverick
'''

"""
#------------ Get OHLC top 15 cryto - exchange Kraken 
Cryto
bitcoin (btc), ethereum (eth), litecoin (ltc), polkadot (dot), monero (xmr), dogecoin (xdg), stellar lumens (xlm),
ripple (xrp), zcash (zec), nano (nano), tron (trx), bitcoin cash (bch), tezos (xtz), cardano (ada), orchid (oxt)
   

path imports.   
add the following bit of code to the beginning of your imports 
(as long as your notebook is inside a folder inside libs) it will tell Python to add the libs folder 
to the list of folders it looks for imports from 
(to see a list of folders Python is searching print sys.path after import.sys):

import sys
import os
module_path = os.path.abspath(os.path.join('../..'))
if module_path not in sys.path:
    sys.path.append(module_path)

Which then will allow you to import from our signals, data, or trading packages, i.e.:
from libs.signals import signals.ewma_crossover
from libs.trading import 'some_trading_function/library'

to see a list of folders Python is searching print sys.path after import.sys):


Note: you can remove a path by doing sys.path.remove('path_you_want_to_remove'), 
but be really careful with that and don't remove any of the default paths!
If y'all would rather just move the files into the libs folder, 

"""

# ---------- imports 
import sys
import os
#module_path = os.path.abspath(os.path.join('../..'))
#if module_path not in sys.path:
#    sys.path.append(module_path)
#sys.path

for p in (sys.path):
    print(p)

os.getcwd()

import pandas as pd 
import numpy as np
import datetime
import time, json, requests, sys
from time import time, ctime
from datetime import datetime
import pathlib
from requests.exceptions import HTTPError
#from libs.signals import signals 



#------------ functions 

#--- Get OHLC from Kraken
def Get_OHLC_Kraken(pair, interval=1440, since=0):
    response_json = {}
    url = 'https://api.kraken.com/0/public/OHLC'  #only last 720 datapoints 

    # note for appending to OHLC Master file because i+1 in jason for end-of day 
    print (f"\nGet OHLC from API note that:") 
    print (f"   In case of appending to Master file --> since = second last date (not last) <-- as we take EOD (i+1 json)--> in this case: {ctime(since)}\n") 

    for i in range(3):
        try:
            response_kraken = requests.post(url, 
            params=
                    {'pair':pair,
                    'interval':interval,        # integer default=1, e.g. interval=60  --> 1 5 15 30 60 240 1440 10080 21600
                    'since':since},             # integer  e.g. interval=60 Return up to 720 OHLC data points since given timestamp
                    headers={"content-type":"application/json"})
                
            # If the response was successful, no Exception will be raised
            response_kraken.raise_for_status()
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')  
        except Exception as err:
            print(f'Other error occurred: {err}')  
        else:
             print('Success, URL found!')
        break
        print('--------------------  try #');print(i)
        time.sleep(5)

    if i == 2: sys.exit('URL error GGC')     

    #print(response_kraken.json())
    #g = input("control 2 : "); print (g)
    return response_kraken.json()


# --- Process OHLC json into a list
def Process_Kraken_Jason(kraken_json, pair):
    i = 0
    l_price = []
    
    while True:
        try:
            new_row = [ctime(kraken_json['result'][pair][i+1][0]),    # Date end day next day 
            #new_row = [ctime(kraken_json['result'][pair][i][0]),    # 
                        kraken_json['result'][pair][i][1],           # Open                      
                        kraken_json['result'][pair][i][2],           # High                       
                        kraken_json['result'][pair][i][3],           # Low                       
                        kraken_json['result'][pair][i][4],           # Close
                        kraken_json['result'][pair][i][6]            # Volume 
                       ] 
            #print (new_row)
            l_price += [new_row]
            i += 1           

        except Exception as err:
            print(f'error Process_Kraken_Jason: {err}')  
            break 
        
    return l_price


# ---  tranform OHLC list into a DataFrame '
def l_price_into_df_price(l_price, pair):
    
    # l_labels = ['Date', 'Open_'+pair[0:5], 'High_'+pair[0:5], 'Low_'+pair[0:5], 'Close_'+pair[0:5], 'Vol_'+pair[0:5]] 
    l_labels = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
    df_price = pd.DataFrame.from_records(l_price, columns=l_labels)   # l --> list, l_labels --> column names 
    df_price['Date'] = pd.to_datetime(df_price['Date'])
    df_price.set_index('Date', inplace=True) 

    df_price[['Open', 'High', 'Low', 'Close', 'Volume']] = df_price[['Open', 'High', 'Low', 'Close', 'Volume']].apply(pd.to_numeric)

    ### df_price = df_price.iloc[:-1,:]            # NO needed anymore,  we've change the logic see date i+1 next day above    # drop last row, we want yesterday, today is starting 

    return(df_price)


# ---- concatenate pair dataframe with the global dataframe 
def Concact_Prices(df_price, ft, df_all_prices):
    if ft == True:
        df_all_prices = pd.concat([df_price], axis="columns", join="outer")
        ft = False
    else:
        df_all_prices = pd.concat([df_all_prices, df_price], axis="columns", join="outer")

    return df_all_prices, ft


# ---------- Export df to CSV 
def export_df_csv(df_all_prices):

    ''' main calls this library, it is like library was in same directory --> ./data/..... '''
    df_all_prices.to_csv('./data/crypto_prices.csv')
    return 


# --------- Main 

def main(crypto_pair, interval=1440, since=0):


    #bitcoin (btc), ethereum (eth), litecoin (ltc), polkadot (dot), monero (xmr), dogecoin (xdg), stellar lumens (xlm),
    #ripple (xrp), zcash (zec), nano (nano), tron (trx), bitcoin cash (bch), tezos (xtz), cardano (ada), orchid (oxt)
    #DOT, XDG, NANO, TRX, and OXT.
    #cryto_list = ['XXBTZUSD', 'XETHZUSD', 'XLTCZUSD', 'DOTUSD', 'XXMRZUSD', 'XDGUSD', 'XXLMZUSD', 'XXRPZUSD', 
    #            'XZECZUSD', 'NANOUSD', 'TRXUSD', 'BCHUSD', 'XTZUSD', 'ADAUSD', 'OXTUSD']
    #cryto_list = ['XXBTZUSD']


    cryto_list = [crypto_pair]
    ft = True 
    df_all_prices = pd.DataFrame()

    for pair in (cryto_list):
        kraken_json = Get_OHLC_Kraken(pair, interval, since)
        #print(kraken_json)

        l_price = Process_Kraken_Jason(kraken_json, pair)
        #print(l_price)

        df_price = l_price_into_df_price(l_price, pair)
        #print(df_price.tail(20))

        df_all_prices, ft = Concact_Prices(df_price, ft, df_all_prices)
    
    
    export_df_csv(df_all_prices)

    return df_all_prices


#------------- 
if __name__ == "__main__":
    cryto_list = ['XXBTZUSD', 'XETHZUSD']
    main(cryto_list)
    g = input("\n\n\n End Program .... Press any key : "); print (g)

