# -*- coding: utf-8 -*-
"""
Created on 2020/2/14

@author: Tony She

E-mail: tony_she@yahoo.com

This module add MA data to the database and will provide support to strategy
based on MA trend.

Functions:

socket() -- create a new socket object
"""

import sqlite3 as db

import trend_functions.get_data as get_data
import pandas as pd


def cal_MA():
    """Calculate SMA5, SMA10, SMA20 for all stocks in database"""

    con = db.connect('D:\\Data\\data.sqlite')
    cur = con.cursor()
    cur.execute("select name from sqlite_master where type='table'")

    # get all table from the database
    stock_list = cur.fetchall()
    stock_list = [line[0] for line in stock_list]

    count = 0
    one_percent = int(len(stock_list) / 100)
    # read the dataframe one by one from the stock_list
    for stock in stock_list:

        # Show the progress of the downloading
        count += 1
        if count % one_percent == 0:
            print(count / one_percent)

        stock_data = pd.read_sql_query(
            sql="SELECT * FROM '" + stock + "'",
            con=con
            )
        for i in [5, 10, 20]:
            SMA = stock_data['close'].rolling(window=i).mean()
            stock_data['SMA%s' % str(i)] = SMA.reset_index(drop=True)

        stock_data.to_sql(
            name=stock,
            con=con,
            if_exists='replace',
            index=False
            )
    cur.close()
    con.close()


def main():
    cal_MA()
    print('done')


if __name__ == '__main__':
    main()
