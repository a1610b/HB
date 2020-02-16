# -*- coding: utf-8 -*-
"""
Created on 2019/7/10

@author: Tony She

E-mail: tony_she@yahoo.com

This module provides socket operations and some related functions.
On Unix, it supports IP (Internet Protocol) and Unix domain sockets.
On other systems, it only supports IP. Functions specific for a
socket are available as methods of the socket object.

Functions:

socket() -- create a new socket object
"""

import logging
import sqlite3 as db

import pandas as pd
import tushare as ts


def get_from_sql(item: str = '*',
                 name: str = 'data',
                 start_date: str = '19910101',
                 minimum_data: int = 0
                 ) -> dict:
    """
    Get dict of Dataframe data from the given sql_path and item

    Args:
        item (str, optional): Column that return. Defaults to '*'.
        name (str, optional): SQL's path. Defaults to 'data'.
        start_date (str, optional): The starting date of data.
            Defaults to '19910101'.
        minimum_data (int, optional): Reject any stock with data number
            less than minimum_data. Defaults to 0.

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

    # read the dataframe one by one from the stock_list
    for stock in stock_list:
        data[stock] = pd.read_sql_query(
            sql="SELECT " + item + " FROM '" + stock + "'",
            con=con
            )
        # print(data[stock])
        if data[stock].shape[0] < minimum_data:
            data.pop(stock)
        # data[stock] = data[stock][data[stock]['trade_date'] > start_date]

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
    industry_stock_list = {}

    for i in df:
        industry_stock_list[i] = pro.index_member(index_code=i)
    return industry_stock_list


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
    stock_list = set(pro.stock_basic(exchange='',
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
    stock_list = list(stock_list)[::-1]
    one_percent = int(len(stock_list) / 100)
    for i in stock_list:
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

            stock_data = df1[set(df1.columns) - {'ts_code'}].merge(
                    df2[set(df2.columns) - {'ts_code', 'close'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stock_data = stock_data.merge(
                    df3[['trade_date', 'adj_factor']],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stock_data = stock_data.merge(
                    df4[set(df4.columns) - {'ts_code'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stock_data = stock_data.merge(
                    df5[set(df5.columns) - {'ts_code'}],
                    how='left',
                    right_on='trade_date',
                    left_on='trade_date'
                    )
            stock_data.dropna(how='all', axis=1, inplace=True)
            stock_data = stock_data.iloc[::-1]
            stock_data.reset_index(drop=True, inplace=True)

            # Ignore stock that don't have data yet
            if stock_data.shape[0] == 0:
                continue

            # Write in the database
            stock_data.to_sql(
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
