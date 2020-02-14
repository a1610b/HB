# -*- coding: utf-8 -*-
"""
Created on 2019/7/10

@author: Tony She

E-mail: tony_she@yahoo.com

This is the computation module and can use cuda and multiprocessing to
increase productivity.

Functions:

socket() -- create a new socket object
"""

import sqlite3 as db

import trend_functions.data_process as data_process
import trend_functions.get_data as get_data
import pandas as pd


def main():
    data = get_data.get_from_sql(name='stock_data')
    resultRaise = pd.DataFrame(columns=[
        'rate of increase', 'increase length', 'rate of decrease',
        'decrease length', 'rate of decrease today', 'change next day', 'bounce back point'
        ])
    resultDrop = pd.DataFrame(columns=[
        'rate of decrease', 'decrease length', 'rate of increase',
        'increase length', 'rate of increase today', 'change next day', 'bounce back point'
        ])
    upAndDown = int(input('连续多少天定义为趋势：'))
    unsureRate = float(input('保证趋势占比为：'))
    dropdata = {}
    raisedata = {}
    con = db.connect('data-up-and-down.sqlite')
    # p = Pool(10)
    for i in data:
        result = data_process.computation(data[i], upAndDown, unsureRate, con, i)
        # p.apply_async(computation, args=(data[i], upAndDown, unsureRate, con, i, ))
        dropdata[i], raisedata[i] = data_process.getInfoForHighLowPoint(result)
        for j in dropdata[i]:
            resultRaise = data_process.getInfoOnRaise(j, resultRaise)
        for j in raisedata[i]:
            resultDrop = data_process.getInfoOnDrop(j, resultDrop)
    con.close()
    resultRaise.to_csv('rasie2.csv')
    resultDrop.to_csv('drop2.csv')


if __name__ == '__main__':
    main()
