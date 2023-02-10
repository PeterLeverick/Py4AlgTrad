""" 
    Author: Peter Leverick Jan 2023 

    Citation --> Python for Algo Trading (O'Reilly) Kindle 

    It requieres the 3.09 environment 
    This version gets OHLC from YAHOO FINANCE csv  --> https://finance.yahoo.com/quote/ETH-USD/history?p=ETH-USD

    Module with Class for Vectorized Backtesting of SMA-based strategies

"""

import pandas as pd
import numpy as np
from numpy.core.fromnumeric import round_
from scipy.optimize import brute

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
#import csv

#------- import libraries 
import kraken_libs

''' Class for the vectorized backtesting of SMA-based trading strategies'''
class MAVectorBacktester(object):

    ''' 
    Attibutes
    symbol: str
    SMA1: int   time window in days for shorter SMA
    SMA2: int   time window in days for longer SMA
    start: str  start date for data 
    
    '''
    def __init__(self, symbol, MA1, MA2, start, end):
        self.symbol = symbol
        self.MA1 = MA1
        self.MA2 = MA2
        self.start = start
        self.end = end
        self.results = None
        self.get_data()
     

    #--- get master ohlc from csv & prepare df 
    def get_data(self):
        path_csv = './data/'
        name_csv = 'master_ohlc_yahoo.csv'
        OHLC_csv = path_csv + name_csv
        asset_df = pd.read_csv(OHLC_csv, index_col = 'Date')

        # Converting the dates from string to datetime format:
        asset_df.index = pd.to_datetime(asset_df.index)
        asset_df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']] = asset_df[['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']].apply(pd.to_numeric)

        #print (asset_df.head())
        #print (asset_df.tail())

        ''' clean df to only one price '''
        what_column = ['Adj Close']         #can be more than one 
        raw_df = asset_df[what_column].copy()
        raw_df.rename(columns = {what_column[0]:'price'}, inplace = True)
        #print (f"\n len df {len(raw_df)}")

        ''' calculate log returns  '''
        raw_df['returns'] = np.log(raw_df['price'] / raw_df['price'].shift(1))

        ''' moving averages  '''
        #ema_span = 50  #ggc
        #sma_span = 200 #ggc 
        raw_df['MA1'] = raw_df['price'].ewm(span=self.MA1).mean()   #exponential
        raw_df['MA2'] = raw_df['price'].rolling(self.MA2).mean()    #simple 
        raw_df.dropna(inplace=True)

        self.data = raw_df


    #--- Backtest the trading strategy 
    def run_strategy(self):

        data = self.data.copy().dropna()
        data['position'] = np.where(data['MA1'] > data['MA2'], 1, -1)
        data['strategy'] = data['position'].shift(1) * data['returns']
        data.dropna(inplace=True)
        data['creturns'] = data['returns'].cumsum().apply(np.exp)
        data['cstrategy'] = data['strategy'].cumsum().apply(np.exp)
        self.results = data 
        #print(self.results)

        #gross performance of the symbol
        aperf = data['creturns'].iloc[-1]
        
        #gross performance strategy
        aperf = data['cstrategy'].iloc[-1]
        #out/underperformance of strategy 
        operf = aperf - data['creturns'].iloc[-1]
        print(f"GM symbol = {round(data['creturns'].iloc[-1],2)},   GM strategy = {round(aperf,2)},    Delta strategy - symbol = {operf,2}")

        return round(data['creturns'].iloc[-1],2), round(aperf,2), round(operf,2)


    #--- Update parameters --> Updates MA parameters and time series --> need to run strategy after again 
    def set_parameters(self, MA1=None, MA2=None):

        if MA1 is not None:
            self.MA1 = MA1
            self.data['MA1'] = self.data['price'].ewm(self.MA1).mean()   #exponential

        if MA2 is not None:
            self.MA2 = MA2
            self.data['MA2'] = self.data['price'].rolling(self.MA2).mean()   #simple


    #--- Update MA parameters and returns negative absolute performance (for minimazation algorithm)
    def update_and_run(self, MA):

        #MA: tuple
        self.set_parameters(int(MA[0]), int(MA[1]))
        return -self.run_strategy()[0]


    #--- Update MA parameters and returns negative absolute performance (for minimazation algorithm)
    def optimize_parameters(self, MA1_range, MA2_range):

        #MA1_range, MA2_range --> tuples of the form (start, end, step size)

        opt = brute(self.update_and_run, (MA1_range, MA2_range), finish=None)
        
        return opt, -self.update_and_run(opt)   # self.MA1, self.MA2 are updated to best values 


    ''' plot Price  <-- Adj Close '''
    def plot_price(self):

        dates = self.data.index
        price = self.data['price']
    
        with plt.style.context('fivethirtyeight'):
            fig = plt.figure(figsize=(14,7))
            plt.plot(dates, price, linewidth=1.5, label='Day Adj Close')
            plt.title("Yahoo Finance ETH-USD Day Adj-Close")
            plt.ylabel('Price($)')
            plt.legend()
    
        plt.show() # This is needed only if not in Jupyter
        return


    ''' plot strategy --> Price, MA1, MA2   '''
    def plot_strategy(self):

        if self.results is None:
            print('No results to plot yet. Run a Strategy')

        dates = self.results.index
        price = self.results['price']
        ma1 = self.results['MA1']
        ma2 = self.results['MA2']
    
        with plt.style.context('fivethirtyeight'):
            fig = plt.figure(figsize=(14,7))
            plt.plot(dates, price, linewidth=1.5, label='Price 1440min Adj Close')
            plt.plot(dates, ma2, linewidth=2, label='SMA')
            plt.plot(dates, ma1, linewidth=2, label='EMA')
            plt.title("EMA, SMA Crossover")
            plt.ylabel('Price($)')
            plt.legend()
    
        plt.show() # This is needed only if not in Jupyter
        return


    #--- Plot the cumulative performance of the trading strategy compared with the symbol 
    def plot_results(self):

        if self.results is None:
            print('No results to plot yet. Run a Strategy')
        title = '%s | MA1=%d, MA2=%d' % (self.symbol, self.MA1, self.MA2)
        self.results[['creturns', 'cstrategy']].plot(title=title, figsize=(10,6))
        plt.show() # This is needed only if not in Jupyter
        return 



  
'''-----------------------------------------------------------------'''
'''                 Main Function                                   '''
'''-----------------------------------------------------------------'''
def main():

    ''' xxx  '''
    #mabt =  MAVectorBacktester('ETH-USD', 42, 252, '2017', '2023') # <-- we know this is not the best
    #mabt =  MAVectorBacktester('ETH-USD', 50, 200, '2017', '2023')  # ggc 
    mabt =  MAVectorBacktester('ETH-USD', 30, 204, '2017', '2023')  # the best from previous optimization 
    #g = input("call class .... Press any key : ")

    print(mabt.run_strategy())
    g = input("run strategy .... Press any key : ") 

    mabt.plot_strategy()
    g = input("plot strategy results .... Press any key : ") 

    mabt.plot_results()
    g = input("plot results .... Press any key : ") 



    print(mabt.optimize_parameters((30, 56, 4), (200, 300, 4)))
    g = input("optimize parameters .... Press any key : ")

    #mabt.plot_price()
    #g = input("plot price = Adj Close .... Press any key : ") 

    mabt.plot_strategy()
    g = input("plot strategy results .... Press any key : ") 

    mabt.plot_results()
    g = input("plot results .... Press any key : ") 

    return

  

    mabt.set_parameters(MA1=20, MA2=100)
    g = input("update parameters .... Press any key : ") 



    return


  
if __name__== "__main__":
  main()
  g = input("End Program backtesting .... Press any key : "); print (g)


