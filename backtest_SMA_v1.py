""" 
    Author: Peter Leverick Nov 2022 

    Citation --> Python for Algo Trading (O'Reilly)
    Kindle 
    
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
#import csv


#------- import libraries 
import kraken_libs


# 1/ ------------   Get prices  

''' get ohlc from kraken '''
def get_ohlc(crypto_pair='ETHUSDT', interval='15'):
    what_column = ['Close']
    asset_df = kraken_libs.kraken_ohlc_lib.main(crypto_pair, interval) 
    last_date_ohlc = asset_df.index[-1]                                 # this should be second last close (last row dropped in ohlc lib)
    print(asset_df)
    print(asset_df.iloc[-1])
    print (f"\n last date {asset_df.index[-1]}  {asset_df[what_column].iloc[-1]}\n")       #last index
    print (f"\n len df {len(asset_df)}")
    return asset_df

''' clean df to only one price '''
def df_one_price(asset_df):
    what_column = ['Close']         #can be more than one 
    df2 = asset_df[what_column].copy()
    df2.rename(columns = {what_column[0]:'Price'}, inplace = True)
    print (f"\n len df {len(df2)}")
    return df2


# 2/ ------------   Compute - Derivate 
''' moving averages  '''
def comp_moving_avegares(df):
    #sma_span = 21
    #ema_span = 9
    #sma_span = 252
    #ema_span = 42
    sma_span = 200
    ema_span = 50

    df = df.apply(pd.to_numeric)
    df['ema_st'] = df['Price'].ewm(span=ema_span).mean()
    df['sma_lt'] = df['Price'].rolling(sma_span).mean()
    df.dropna(inplace=True)
    df.round(2)
    print(df) 
    print (f"\n len df {len(df)}")
    #print(df.info())

    return df 

''' derive position as per Moving Averages crossover  '''
def derive_position(df):
    df['Position'] = np.where(df['ema_st'] > df['sma_lt'], 1, -1)
    df.round(3)
    print(df) 

    #df['equal_0?'] = df['Position'].apply(lambda x: 'True' if x == 0 else 'False')
    #filtered_data = df[df["Position"]==0]
    #print (filtered_data)

    print(f"len df: {len(df)}")
    print(f"# of longs: {len(df[df['Position']==1])}")
    print(f"# of shorts: {len(df[df['Position']==-1])}")

    return df 

''' calculate log returns  '''
def log_returns(df):
    df['Returns'] = np.log(df['Price'] / df['Price'].shift(1))
    df.round(3)
    print(df) 
    print(f"len df: {len(df)}")

    return df 

''' compute strategy returns  '''
def strategy_returns(df):
    df['Strategy'] = df['Position'].shift(1) * df ['Returns']
    df.round(3)
    print(df) 
    print(f"len df: {len(df)}")

    return df 

''' compute strategy returns  '''
def stock_returns_vs_strategy_returns(df):
    #sump up the single log return values for stock returns and strategy returns
    print(f"Returns, Strategy: {df[['Returns', 'Strategy']].sum()}")

    #apply exponential function to the sum of the log returns to 
    #calculate the gross performance of the stock returns and strategy returns
    print(f"Gross Returns, Strategy:{df[['Returns', 'Strategy']].sum().apply(np.exp)}")

    return 

''' annualized risk-return statistics for both stock an strategy  '''
def risk_return_statistics(df):
    #Calculates the annualized mean return for both log and regular space 
    print(f"mean for Hold Returns vs Strategy: {df[['Returns', 'Strategy']].mean()*252}")
    print(f"log mean for Hold Returns vs Strategy:{np.exp(df[['Returns', 'Strategy']].mean()*252) -1}")

    #Calculates the annualized standard deviation return for both log and regular space
    print(f"SD for Hold Returns vs Strategy: {df[['Returns', 'Strategy']].std() * 252 ** 0.5}")
    print(f"log SD for Hold Returns vs Strategy: {(df[['Returns', 'Strategy']].apply(np.exp) - 1).std() * 252 ** 0.5}")
    return 



# 3 ---------------- Plot 
''' plot Price, EMA, SMA   '''
def plot_system1(data):
    df = data.copy()
    dates = df.index
    price = df['Price']
    sma200 = df['sma_lt']
    ema20 = df['ema_st']
    
    with plt.style.context('fivethirtyeight'):
        fig = plt.figure(figsize=(14,7))
        plt.plot(dates, price, linewidth=1.5, label='Price 1440min Close')
        plt.plot(dates, sma200, linewidth=2, label='SMA')
        plt.plot(dates, ema20, linewidth=2, label='EMA')
        plt.title("EMA, SMA Crossover")
        plt.ylabel('Price($)')
        plt.legend()
    
    plt.show() # This is needed only if not in Jupyter
    return

''' plot returns histogram   '''
def plot_returns_hist(data):
    df = data.copy()
    returns = df['Returns']

    with plt.style.context('fivethirtyeight'):
        fig, ax = plt.subplots()
        ax.hist(returns, bins=35, linewidth=0.5, edgecolor="white")
        ax.set(xlim=(-0.2, 0.2), xticks=np.linspace(-0.2, 0.2, 10),
        ylim=(0, 120), yticks=np.linspace(0, 120, 20)) 

    plt.show() # This is needed only if not in Jupyter
    return

''' plot gross performance stock vs strategy   '''
def plot_gross_performance(data):
    df = data.copy()
    dates = df.index
    returns = df[['Returns']].cumsum().apply(np.exp)
    strategy = df[['Strategy']].cumsum().apply(np.exp)
    
    with plt.style.context('fivethirtyeight'):
        fig = plt.figure(figsize=(14,7))
        plt.plot(dates, returns, linewidth=1.5, label='Stock Returns')
        plt.plot(dates, strategy, linewidth=2, label='Strategy Returns')
        plt.title("Gross Performance Stock vs Strategy")
        plt.ylabel('Price($)')
        plt.legend()
    
    plt.show() # This is needed only if not in Jupyter
    return



'''-----------------------------------------------------------------'''
'''                 Main Function                                   '''
'''-----------------------------------------------------------------'''
def main():

    ''' get ohlc from kraken '''
    crypto_pair = 'ETHUSDT'
    interval = '1440'
    asset_df = get_ohlc(crypto_pair, interval)
    g = input("get ohlc .... Press any key : ")

    ''' clean df to only one price Close'''
    asset_df2 = df_one_price(asset_df)
    print(asset_df2.head())
    g = input("df_one_price .... Press any key : ")

    ''' moving averages  '''
    asset_df2 = comp_moving_avegares(asset_df2)
    g = input("moving averages .... Press any key : ")

    ''' plot Price, SMA, EMA  '''
    plot_system1(asset_df2)
    g = input("plot Price, SMA, EMA .... Press any key : ")

    ''' derive position (1, -1) by EMA and SMA crossover  '''
    asset_df2 = derive_position(asset_df2)
    g = input("derive position .... Press any key : ")

    ''' compute log returns   '''
    asset_df2 = log_returns(asset_df2)
    g = input("log returns .... Press any key : ")

    ''' plot returns histogram  '''
    plot_returns_hist(asset_df2)
    g = input("plot returns histogram .... Press any key : ")

    ''' derive strategy returns   '''
    asset_df2 = strategy_returns(asset_df2)
    g = input("strategy returns .... Press any key : ")

    ''' derive strategy returns   '''
    asset_df2 = strategy_returns(asset_df2)
    g = input("strategy returns .... Press any key : ")

    ''' compare stock returns vs strategy returns  '''
    stock_returns_vs_strategy_returns(asset_df2)
    g = input("stock returns vs strategy returns .... Press any key : ")

    ''' plot gross performance, stock vs strategy   '''
    plot_gross_performance(asset_df2)
    g = input("plot gross performance stock vs strategy .... Press any key : ")

    ''' print annualized risk-return statistics   '''
    risk_return_statistics(asset_df2)
    g = input("annualized risk-return statistics .... Press any key : ")


    return


  
if __name__== "__main__":
  main()
  g = input("End Program backtesting .... Press any key : "); print (g)






'''
    #get ohlc from csv 
    asset_df = get_ohlc_CSV()
    asset_df2 = asset_df.copy()


def get_ohlc_CSV():
    #datafile = '.data/ohlc2021.csv'
    # CSV with OHLC
    path_csv = './data/'
    name_csv = 'ohlc2021.csv'
    OHLC_csv = path_csv + name_csv
    asset_df = pd.read_csv(OHLC_csv, index_col = 'Date')
    # Converting the dates from string to datetime format:
    asset_df.index = pd.to_datetime(asset_df.index)
    asset_df[['Open', 'High', 'Low', 'Close']] = asset_df[['Open', 'High', 'Low', 'Close']].apply(pd.to_numeric)
    asset_df = asset_df.iloc[:-1,:]            # drop last row, still wip 
    last_date_ohlc = asset_df.index[-1]                                 # this should be second last close (last row dropped in ohlc lib)
    
    print(asset_df)
    print(asset_df.iloc[-1])
    print (f"\n last date {asset_df.index[-1]}  {asset_df['Close'].iloc[-1]}\n")       #last index
    return asset_df


### trading condition  
def trading_condition(df):
    long_positions = np.where(df['ema20'] > df['sma200'], 1, 0)
    df['Position'] = long_positions
    df.round(3)
    print(df) 

    #df['equal_0?'] = df['Position'].apply(lambda x: 'True' if x == 0 else 'False')
    filtered_data = df[df["Position"]==0]
    print (filtered_data)

    return df 

### select days when trading signal is triggered  
def buy_signal_triggered(df):
    buy_signals = (df["Position"] == 1) & (df["Position"].shift(1) == 0)
    print(df.loc[buy_signals].round(3)) 

    buy_signals_prev = (df["Position"].shift(-1) == 1) & (df["Position"] == 0)
    print(df.loc[buy_signals | buy_signals_prev].round(3))

    return 

## returns buy & hold vs moving average strategy 
def returns_long(df):
    #returns of the Buy and Hold strategy
    df['Hold'] = np.log(df['Close'] / df['Close'].shift(1))
    
    # The returns of the Moving Average strategy:
    df['Strategy'] = df['Position'].shift(1) * df['Hold']
    # We need to get rid of the NaN generated in the first row:
    df.dropna(inplace=True)
    
    print(df)
    returns = np.exp(df[['Hold', 'Strategy']].sum()) - 1
    print(f"Buy and hold return: {round(returns['Hold']*100,2)}%")
    print(f"Strategy return: {round(returns['Strategy']*100,2)}%")

    print(f"len of df = {len(df)} this is {len(df)/96} days")
    n_days = len(df)/96   #96 as we are in 15min
    # Assuming 252 trading days in a year:
    ann_returns = 252 / n_days * returns
    
    print(f"Buy and hold annualized return: {round(ann_returns['Hold']*100,2)}%")
    print(f"Strategy annualized return:{round(ann_returns['Strategy']*100,2)}%")
    return df



### plot 2 with signals    
def plot_system1_sig(data):
    df = data.copy()
    dates = df.index
    price = df['Close']
    sma200 = df['sma200']
    ema20 = df['ema20']
    
    buy_signals = (df['Position'] == 1) & (df['Position'].shift(1) == 0)
    buy_marker = sma200 * buy_signals - (sma200.max()*.05)
    buy_marker = buy_marker[buy_signals]
    buy_dates = df.index[buy_signals]
    sell_signals = (df['Position'] == 0) & (df['Position'].shift(1) == 1)
    sell_marker = sma200 * sell_signals + (sma200.max()*.05)
    sell_marker = sell_marker[sell_signals]
    sell_dates = df.index[sell_signals]
    
    with plt.style.context('fivethirtyeight'):
        fig = plt.figure(figsize=(14,7))
        plt.plot(dates, price, linewidth=1.5, label='CPB price - Daily Adj Close')
        plt.plot(dates, sma200, linewidth=2, label='200 SMA')
        plt.plot(dates, ema20, linewidth=2, label='20 EMA')
        plt.scatter(buy_dates, buy_marker, marker='^', color='green', s=160, label='Buy')
        plt.scatter(sell_dates, sell_marker, marker='v', color='red', s=160, label='Sell')
        plt.title("A Simple Crossover System with Signals")
        plt.ylabel('Price($)')
        plt.legend()
    
    plt.show() # This is needed only if not in Jupyter
    return 



### candlesticks with mlpfinance ###
def plot_system2_ls(data):
    df2 = data.copy()
    dates = np.arange(len(df2)) # We need this for mpl.plot()
    price = df2['Close']
    h_sma = df2['H_sma']*1.02
    l_sma = df2['L_sma']*.98
    c_ema = df2['C_ema']
    
    with plt.style.context('fivethirtyeight'):
        fig = plt.figure(figsize=(14,7))
        ax = plt.subplot(1,1,1)
        mpf.plot(df2, ax=ax, show_nontrading=False, type='candle')
        ax.plot(dates, h_sma, linewidth=2, color='blue', label='High 40 SMA + 2%')
        ax.plot(dates, l_sma, linewidth=2, color='blue', label='Low 40 SMA - 2%')
        ax.plot(dates, c_ema, linewidth=1.5, color='red', linestyle='--', label='Close 15 EMA')
        plt.title("A System with Long-Short Positions")
        ax.set_ylabel('Price($)')
        plt.legend()
    
    plt.show() # This is needed outside of Jupyter

### trading condition ###
def trading_condition_ls(df):
    offset = 0.02
    long_positions = np.where(df['C_ema'] > df['H_sma']*(1+offset), 1, 0)
    short_positions = np.where(df['C_ema'] < df['L_sma']*(1-offset), -1, 0)
    df['Position'] = long_positions + short_positions.round(3)

    print(df) 

    #df['equal_0?'] = df['Position'].apply(lambda x: 'True' if x == 0 else 'False')
    filtered_data = df[df["Position"]==-1]
    print (filtered_data)

    print(f"number of total rows in df = {len(df)}")

    return df 

def plot_system2_ls_signals(data):
    df2 = data.copy()
    dates = np.arange(len(df2)) # We need this for mpl.plot()
    price = df2['Close']
    h_sma = df2['H_sma']*1.02
    l_sma = df2['L_sma']*.98
    c_ema = df2['C_ema']
    
    def reindex_signals(signals, markers):
        ###
        - takes two pd.Series (boolean, float)
        - returns signals and markers reindexed to an integer range, and their index
       ###
        signals.reset_index(drop=True, inplace=True)
        signals = signals[signals==True]
        dates = signals.index
        markers = markers[dates]
        markers.index = dates
        return signals, markers, dates
    
    buy_signals = (df2['Position'] == 1) & (df2['Position'].shift(1) != 1)
    buy_marker = h_sma * buy_signals[buy_signals==True] - (h_sma.max()*.04)
    buy_signals, buy_marker, buy_dates = reindex_signals(buy_signals, buy_marker)
    
    exit_buy_signals = (df2['Position'] != 1) & (df2['Position'].shift(1) == 1)
    exit_buy_marker = h_sma * exit_buy_signals + (h_sma.max()*.04)
    exit_buy_signals, exit_buy_marker, exit_buy_dates = reindex_signals(exit_buy_signals, exit_buy_marker)
    
    sell_signals = (df2['Position'] == -1) & (df2['Position'].shift(1) != -1)
    sell_marker = l_sma * sell_signals + (l_sma.max()*.04)
    sell_signals, sell_marker, sell_dates = reindex_signals(sell_signals, sell_marker)
    
    exit_sell_signals = (df2['Position'] != -1) & (df2['Position'].shift(1) == -1)
    exit_sell_marker = l_sma * exit_sell_signals - (l_sma.max()*.04)
    exit_sell_signals, exit_sell_marker, exit_sell_dates = reindex_signals(exit_sell_signals, exit_sell_marker)
    
    with plt.style.context('fivethirtyeight'):
        fig = plt.figure(figsize=(14,7))
        ax = plt.subplot(1,1,1)
        mpf.plot(df2, ax=ax, show_nontrading=False, type='candle')
        ax.plot(dates, h_sma, linewidth=2, color='blue', label='High 40 SMA + 2%')
        ax.plot(dates, l_sma, linewidth=2, color='blue', label='Low 40 SMA - 2%')
        ax.plot(dates, c_ema, linewidth=1.5, color='red', linestyle='--', label='Close 15 EMA')
        ax.scatter(buy_dates, buy_marker, marker='^', color='green', s=160, label='Buy')
        ax.scatter(exit_buy_dates, exit_buy_marker, marker='v', s=160, label='Exit Buy')
        ax.scatter(sell_dates, sell_marker, marker='v', color='red', s=160, label='Sell')
        ax.scatter(exit_sell_dates, exit_sell_marker, marker='^', color='orange', s=160, label='Exit Sell')
        plt.title("A System with Long-Short Signals")
        ax.set_ylabel('Price($)')
        plt.legend()
    
    plt.show() # This is needed outside of Jupyter

    return

## returns buy & hold  vs long & short  
def returns_ls(data):

    df = data.copy()
    # The returns of the Buy and Hold strategy:
    df['Hold'] = np.log(df['Close'] / df['Close'].shift(1))
    
    # The returns of the Moving Average strategy:
    df['Strategy'] = df['Position'].shift(1) * df['Hold']
    
    # We need to get rid again of the NaN generated in the first row:
    df.dropna(inplace=True)

    print(df)

    returns = np.exp(df[['Hold', 'Strategy']].sum()) -1
    print(f"Buy and hold return: {round(returns['Hold']*100,2)}%")
    print(f"Strategy return: {round(returns['Strategy']*100,2)}%")


    print(f"len of df = {len(df)} this is {len(df)/96} days")
    n_days = len(df)/96   #96 as we are in 15min
    # Assuming 252 trading days in a year:
    ann_returns = 252 / n_days * returns
    
    print(f"Buy and hold annualized return: {round(ann_returns['Hold']*100,2)}%")
    print(f"Strategy annualized return:{round(ann_returns['Strategy']*100,2)}%")

    return df


###  macd signal
def get_macd(asset_df, period_slow=26, period_fast=12, period_signal=9):

    macd_df = signals_libs.signals_lib.macd(asset_df, period_slow=26, period_fast=12, period_signal=9)

    print(macd_df.head(25))
    g = input("\n  macd df .... Press any key : ")

    #macd_signal = macd_df['macd_signal'].iloc[-1]
    #condiv_signal = macd_df['condiv_signal'].iloc[-1]
    #condiv_signal_previous = macd_df['condiv_signal'].iloc[-2]
    #signal = macd_df['signal'].iloc[-1]
    #print(f"macd_signal = {macd_signal}")
    #print(f"condiv_signal = {condiv_signal}")
    #print(f"condiv_signal_previous = {condiv_signal_previous}")
    #print(f"signal = {signal}")
    #g = input("\n return from macd .... Press any key : ")

    return macd_df

## select/verify days when trading signal is triggered
def macd_signals(df):
    
    # ----- buy signals 
    # I am in row 2, if I shift the index (all df) previous row is now current 
    buy_signals = (df["signal"] == 1) & (df["signal"].shift(1) != 1)    #by shiftting 1 previous row becomes current 
    print(df.loc[buy_signals].round(3)) 
    g = input("buy_signals .... Press any key : ")

    buy_signals_prev = (df["signal"] != 1) & (df["signal"].shift(-1) == 1) #by shiftting -1 next row becomes current
    print(df.loc[buy_signals | buy_signals_prev].round(3))
    g = input("buy_signals |  buy_signals_prev .... Press any key : ")
    
    exit_buy_signals = (df['signal'] != 1) & (df['signal'].shift(1) == 1) #by shiftting 1 previous row becomes current
    print(df.loc[buy_signals | buy_signals_prev | exit_buy_signals].round(3).head(25))
    g = input("buy_signals |  buy_signals_prev | exit_buy_signals .... Press any key : ")

    
    # ----- sell signals
    sell_signals = (df['signal'] == -1) & (df['signal'].shift(1) != -1)
    print(df.loc[sell_signals].round(3))
    g = input("sell_signals .... Press any key : ")

    sell_signals_prev = (df["signal"].shift(-1) == -1) & (df["signal"] != -1)
    print(df.loc[sell_signals | sell_signals_prev].round(3))
    g = input("sell_signals |  sell_signals_prev .... Press any key : ")

    exit_sell_signals = (df['signal'] != -1) & (df['signal'].shift(1) == -1)
    print(df.loc[sell_signals | sell_signals_prev | exit_sell_signals].round(3))
    g = input("sell_signals |  sell_signals_prev | exit_sell_signals .... Press any key : ")


    return

## returns buy & hold  vs long & short
def returns_macd(data):

    df = data.copy()
    # The returns of the Buy and Hold strategy:
    df['Hold'] = np.log(df['close'] / df['close'].shift(1))
    
    # The returns of the Moving Average strategy:
    df['Strategy'] = df['signal'].shift(1) * df['Hold']
    print(df['signal'])
    print(df['signal'].shift(1))
    print(df['Hold'])
    g = input("row returns .... Press any key : ")
    
    # We need to get rid again of the NaN generated in the first row:
    df.dropna(inplace=True)

    print(df.head(25))
    g = input("returns .... Press any key : ")

    returns = np.exp(df[['Hold', 'Strategy']].sum()) -1
    print(f"Buy and hold return: {round(returns['Hold']*100,2)}%")
    print(f"Strategy return: {round(returns['Strategy']*100,2)}%")

    print(f"len of df = {len(df)} this is {len(df)/96} days")
    n_days = len(df)/96   #96 as we are in 15min
    # Assuming 252 trading days in a year:
    ann_returns = 252 / n_days * returns
    
    print(f"Buy and hold annualized return: {round(ann_returns['Hold']*100,2)}%")
    print(f"Strategy annualized return:{round(ann_returns['Strategy']*100,2)}%")

    g = input("returns .... Press any key : ")

    return df


'''