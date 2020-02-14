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

socket() -- 
"""

import sys
import trend_functions.data_process as data_process
import trend_functions.get_data as get_data
import pandas as pd
from multiprocessing import Pool

sys.path.append("..")


def throw_error(e):
    print(e)
    raise e


def to_excel(upAndDown):
    conpath = 'data//' + str(upAndDown) + 'dayAsTrend.sqlite'
    data = get_data.getDataFromsql(name = conpath)
    dropdata = {}
    raisedata = {}
    resultRaise = pd.DataFrame(columns=[
            'rate of increase', 'increase length', 'rate of decrease',
            'decrease length', 'rate of decrease today', 'change next day', 'bounce back point'
            ])
    resultDrop = pd.DataFrame(columns=[
            'rate of decrease', 'decrease length', 'rate of increase',
            'increase length', 'rate of increase today', 'change next day', 'bounce back point'
            ])
    
    for i in data:
        dropdata[i], raisedata[i] = data_process.getInfoForHighLowPoint(data[i], i)
        for j in dropdata[i]:
            resultRaise = data_process.getInfoOnRaise(j, resultRaise)
        for j in raisedata[i]:
            resultDrop = data_process.getInfoOnDrop(j, resultDrop)
    # con.close()
    # conclean.close()
    resultRaise.to_csv('data//' + str(upAndDown) +'rasie.csv')
    resultDrop.to_csv('data//' + str(upAndDown) +'drop.csv')


def work(data):
    #upAndDown = int(input('连续多少天定义为趋势：'))
    #unsureRate = float(input('保证趋势占比为：'))
    unsureRate = 0.75
    for upAndDown in range(6, 16):
        #con = db.connect('data//' + str(upAndDown) +'dayAsTrend.sqlite')
        #conclean = db.connect('data//' + str(upAndDown) + 'dayAsTrendCleaned.sqlite')
        #result = {}
        p = Pool(10)
        for i in data:
            #result = dp.computation(data[i], upAndDown, unsureRate, con, i)
            result = p.apply_async(data_process.computation, args=(data[i], upAndDown, unsureRate, 'data//' + str(upAndDown) +'dayAsTrend.sqlite', i, ), error_callback=throw_error)
        print('Waiting for all subprocesses done...')
        p.close()
        p.join()
        print('All subprocesses done.')


def add_label():
    data = get_data.getDataFromsql(item = 'trade_date, close, turnover_rate_f')
    work(data)


def main():
    to_excel(upAndDown=10)


if __name__ == '__main__':
    main()
