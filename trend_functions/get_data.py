# -*- coding: utf-8 -*-
"""
Created on %(date)s

@author: Tony She

E-mail: tony_she@yahoo.com

This module provides socket operations and some related functions.
On Unix, it supports IP (Internet Protocol) and Unix domain sockets.
On other systems, it only supports IP. Functions specific for a
socket are available as methods of the socket object.

Functions:

socket() -- create a new socket object
socketpair() -- create a pair of new socket objects [*]
fromfd() -- create a socket object from an open file descriptor [*]
fromshare() -- create a socket object from data received from socket.share() [*]
gethostname() -- return the current hostname
gethostbyname() -- map a hostname to its IP number
gethostbyaddr() -- map an IP number or hostname to DNS info
getservbyname() -- map a service name and a protocol name to a port number
getprotobyname() -- map a protocol name (e.g. 'tcp') to a number
ntohs(), ntohl() -- convert 16, 32 bit int from network to host byte order
htons(), htonl() -- convert 16, 32 bit int from host to network byte order
inet_aton() -- convert IP addr string (123.45.67.89) to 32-bit packed format
inet_ntoa() -- convert 32-bit packed format IP to string (123.45.67.89)
socket.getdefaulttimeout() -- get the default timeout value
socket.setdefaulttimeout() -- set the default timeout value
create_connection() -- connects to an address, with an optional timeout and
                       optional source address.
"""

import logging

import pandas as pd
import tushare as ts
import sqlite3 as db


def get_from_sql(item: str = '*',
                 name: str = 'stock_data',
                 start_date: str = '19910101',
                 minimum_data: int = 100
                 ) -> dict:
    """
    Get dict of Dataframe data from the given sql_path and item

    Args:
        item (str, optional): Column that return. Defaults to '*'.
        name (str, optional): SQL's path. Defaults to 'stock_data'.
        start_date (str, optional): The starting date of data.
            Defaults to '19910101'.
        minimum_data (int, optional): Reject any stock with data number
            less than minimum_data. Defaults to 100.

    Returns:
        data (dict): dict of Dataframe.

    """

    con = db.connect('D:\\Data\\' + name + '.sqlite')
    cur = con.cursor()
    data = {}
    cur.execute("select name from sqlite_master where type='table'")

    # get all table from the database
    stock_list = cur.fetchall()
    stock_list = [line[0] for line in stock_list]

    # read the dataframe one by one from the stocklist
    for stock in stock_list:
        data[stock] = pd.read_sql_query(
            sql="SELECT " + item + " FROM '" + stock + "'",
            con=con
            )
        data[stock] = data[stock][data[stock]['trade_date'] > start_date]
        if data[stock].shape[0] < 100:
            data.pop(stock)

    cur.close()
    con.close()
    return data


def get_industry_stock_list() -> dict:
    """
    Return a dict consist with list of stock code in each SW Level 1 Industry
    """

    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()
    df = pro.index_classify(level='L1', src='SW')['index code']
    industry_stockList = {}

    for i in df:
        industry_stockList[i] = pro.index_member(index_code=i)
    return industry_stockList


# This is an old function that reads data from local csc files.
# Temporarily removed.
'''
def getDataFromeCSV():

    filename = input('filename: ')
    data = {}
    data['first'] = pd.read_csv(filename)
    data['first'].columns = ['close']
    data['first']['trade_date'] = pd.date_range(
        start='2019-1-09',
        periods=data['first'].shape[0],
        freq='-1D'
        )
    return data
'''


def download_all_market_data(sqlname: str = 'data'):
    """
    Download all stock's data exist in tushare database.

    Args:
        sqlname (str, optional): The name of the local db. Defaults to 'data'.

    """

    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()
    LOG_FORMAT = "%(asctime)s====%(levelname)s++++%(message)s"
    logging.basicConfig(filename="download.log",
                        level=logging.ERROR,
                        format=LOG_FORMAT)

    # Get stock ID of the stock that are now trading(L), suspend trading(P)
    # and delisted(D)
    stockList = set(pro.stock_basic(exchange='',
                                    list_status='D',
                                    fields='ts_code')['ts_code']) \
        | set(pro.stock_basic(exchange='',
                              list_status='L',
                              fields='ts_code')['ts_code']) \
        | set(pro.stock_basic(exchange='',
                              list_status='P',
                              fields='ts_code')['ts_code'])

    con = db.connect('D:\\Data\\'+sqlname+'.sqlite')
    cur = con.cursor()

    count = 0
    stockList = list(stockList)[::-1]
    one_percent = int(len(stockList) / 100)
    for i in stockList:
        try:
            # Avoid calling tushare too frequent
            count += 1
            '''
            if count % 10 == 0:
                time.sleep(10)
            '''

            # Show the progress of the downloading
            if count % one_percent == 0:
                print(count / one_percent)

            df1 = pro.daily(ts_code=i)           # Basic price data
            df2 = pro.daily_basic(ts_code=i)     # Fundamental data
            df3 = pro.adj_factor(ts_code=i)      # Price correction factor
            df4 = pro.moneyflow(ts_code=i)       # Cashflow trend
            df5 = pro.margin_detail(ts_code=i)   # Margin trading data

            stockData = df1[set(df1.columns) - {'ts_code'}].merge(
                    df2[set(df2.columns) - {'ts_code', 'close'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stockData = stockData.merge(
                    df3[['trade_date', 'adj_factor']],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stockData = stockData.merge(
                    df4[set(df4.columns) - {'ts_code'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stockData = stockData.merge(
                    df5[set(df5.columns) - {'ts_code'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stockData.dropna(how='all', axis=1, inplace=True)

            # Ignore stock that don't have data yet
            if stockData.shape[0] == 0:
                continue

            # Write in the database
            if 'trade_date' in stockData.columns:
                stockData.set_index('trade_date', inplace=True)
                stockData.to_sql(
                    name=i,
                    con=con,
                    if_exists='replace',
                    index=True
                    )
            else:
                stockData.to_sql(
                    name=i,
                    con=con,
                    if_exists='replace',
                    index=False
                    )
            con.commit()
        except Exception:
            print(i)
            logging.error('%s', i)
    cur.close()
    con.close()
    return 'Done'


def main():
    download_all_market_data()
    print('done')


if __name__ == '__main__':
    main()
