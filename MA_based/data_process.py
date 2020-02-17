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
import math

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


def cal_return_simple_strategy():
    """    """
    buy_fee = 0.0003
    sell_fee = 0.0013

    result = pd.DataFrame(
        columns=['stock_id', 'investment length', 'annual return'])
    data = get_data.get_from_sql(
        item='trade_date, close, high, low, adj_factor, pct_chg',
        minimum_data=300)
    # con = db.connect('D:\\Data\\SMA_result.sqlite')

    for stock in data:
        df = data[stock]
        df['adj_close'] = df['close'] * df['adj_factor']
        for i in [5, 20]:
            SMA = df['adj_close'].rolling(window=i).mean()
            df['SMA%s' % str(i)] = SMA.reset_index(drop=True)

        df['lower_than_SMA5_yes'] = (df['SMA5'] > df['adj_close']).shift(1)
        df['higher_than_SMA20_yes'] = (df['SMA20'] < df['adj_close']).shift(1)

        # intialize position and cash
        cash = [1000000]
        position = [0]

        for index, row in df.iterrows():
            # Determine if we should buy stock
            if (row['adj_close'] > row['SMA5'])\
                    and row['lower_than_SMA5_yes']\
                    and (position[-1] == 0)\
                    and abs(row['pct_chg']) < 9.8:
                buy_share = math.floor(
                    cash[-1] / row['adj_close'] / (1 + buy_fee))
                position.append(buy_share)
                cash.append(
                    cash[-1] - buy_share * row['adj_close'] * (1 + buy_fee))

            # Determin if we should sell stock
            elif (row['adj_close'] < row['SMA20'])\
                    and row['higher_than_SMA20_yes']\
                    and (position[-1] != 0)\
                    and abs(row['pct_chg']) < 9.8:
                cash.append(cash[-1]
                            + position[-1] * row['adj_close'] * (1 - sell_fee))
                position.append(0)
            else:
                cash.append(cash[-1] * 1.0001)
                position.append(position[-1])

        df['cash'] = cash[1:]
        df['position'] = position[1:]
        investment_length = int(
            (int(df.iloc[-1, 0]) - int(df.iloc[0, 0]))/10000)
        if investment_length == 0:
            investment_length = int(
                (int(df.iloc[-1, 0]) - int(df.iloc[0, 0]))/100) / 12.0

        df['portfolio value'] = df['cash'] + df['position'] * df['adj_close']
        ann_return = math.pow(
            df.iloc[-1, -1] / df.iloc[0, -1],
            1.0 / investment_length
            )
        result = result.append({
            'stock_id': stock,
            'investment length': investment_length,
            'annual return': ann_return
            }, ignore_index=True)
    result.to_csv('result_withfee.csv')
    return None


def main():
    cal_return_simple_strategy()
    print('done')


if __name__ == '__main__':
    main()
