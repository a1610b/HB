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

import sqlite3 as db
import math
import time

import pandas as pd
import tushare as ts
from sklearn.tree import export_graphviz
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
import pydotplus


# 从本地数据库导入数据
def getDataFromsql():
    con = db.connect('D:\\Data\\data.sqlite')
    cur = con.cursor()
    data = {}
    cur.execute("select name from sqlite_master where type='table'")
    tab_name = cur.fetchall()
    tab_name = [line[0] for line in tab_name]
    for i in tab_name:
        data[i] = pd.read_sql_query("SELECT trade_date, close FROM '" + i + "'",
                                    con)
        data[i] = data[i][data[i]['trade_date'] > '20090101']
        if data[i].shape[0] < 100:
            data.pop(i)
    cur.close()
    con.close()
    return data
#从tushare导入数据

def getDataFromTushare():
    stockData = {}
    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    stockList = data['ts_code']
    for i in stockList:

        #由于每次最多只能得到4000条数据，故最多只到2002年的数据
        startDate = '20090101'
        endDate = '20190701'

        stockData[i] = pro.daily(ts_code=i, start_date=startDate, end_date=endDate)[[
            'trade_date', 'open', 'high', 'low', 'close', 'amount'
        ]]
        if stockData[i].shape[0] < 100:
            stockData.pop(i)
    return stockData


#从本地csv导入数据
#csv中只有一列为价格，价格应为从新到旧
def getDataFromeCSV():
    filename = input('filename: ')
    data = {}
    data['first'] = pd.read_csv(filename)
    data['first'].columns = ['close']
    data['first']['trade_date'] = pd.date_range(start='2019-1-09',periods=data['first'].shape[0], freq='-1D')
    return data


# ## 定义函数

# ### 数据处理

#set up or down for each day
def giveUpOrDown(timeSeries):
    timeSeries['upOrDown'] = timeSeries['close'] - timeSeries['close'].shift(
        -1) < 0
    timeSeries['upOrDown'] = timeSeries['upOrDown'].astype("int")
    timeSeries['upOrDown'][timeSeries['upOrDown'] == 0] = -1
    return timeSeries


#给定趋势并且检验合并周期是否合适
def func(timeSeries, Up, Down, mergeDay):
    timeSeries = giveUpOrDown(timeSeries)
    temp = 0
    temp1 = pd.Series(index=timeSeries.index).fillna(0)
    for i in timeSeries.index:
        if temp >= 0:
            if timeSeries.loc[i]['upOrDown'] > 0:
                temp += 1
                if temp == Up:
                    for j in range(Up):
                        temp1[i + j * mergeDay] = 1
                if temp >= Up:
                    temp1[i] = 1
            else:
                temp = -1
        else:
            if timeSeries.loc[i]['upOrDown'] < 0:
                temp -= 1
                if temp == -Down:
                    for j in range(Down):
                        temp1[i + j * mergeDay] = -1
                if temp <= -Down:
                    temp1[i] = -1
            else:
                temp = 1
    timeSeries['Trend'] = temp1
    rate = timeSeries.groupby(['Trend']).size() / timeSeries.shape[0]
    return [rate[0], timeSeries]
    #print('Up rate is ' + str(rate[1]), 'Down rate is ' + str(rate[-1]), 'Unsure rate is '+ str(rate[0]))


#定义高点/低点和趋势的起点
def givePosition(result):
    temp = []
    start = time.clock()
    for i in result.index:
        if i == result.index[-1]:
            if result.loc[i]['Trend'] > 0:
                temp.append('highPoint')
            elif result.loc[i]['Trend'] < 0:
                temp.append('lowPoint')
            else:
                temp.append('float')
        elif (result.loc[i]['Trend'] > 0 and result.loc[i + 1]['Trend'] <= 0):
            temp.append('highPoint')
        elif (result.loc[i]['Trend'] < 0 and result.loc[i + 1]['Trend'] >= 0):
            temp.append('lowPoint')
        elif i != 0 and (result.loc[i]['Trend'] > 0
                         and result.loc[i - 1]['Trend'] <= 0):
            temp.append('beginRaise')
        elif i != 0 and (result.loc[i]['Trend'] < 0
                         and result.loc[i - 1]['Trend'] >= 0):
            temp.append('beginDrop')
        elif i == 0 and result.loc[i]['Trend'] > 0:
            temp.append('beginRaise')
        elif i == 0 and result.loc[i]['Trend'] < 0:
            temp.append('beginDrop')
        elif result.loc[i]['Trend'] > 0:
            temp.append('raising')
        elif result.loc[i]['Trend'] < 0:
            temp.append('droping')
        else:
            temp.append('float')
    result['position'] = temp
    #print(time.clock()-start)
    adjust(result)

#调整保证起始点和高点/低点都在正确的日线位置上
def adjust(result):
    i = 0
    temp = result['position']
    last = result.index[-1]
    while i <= last:
        #Check for if there is higher point
        if temp[i] == 'highPoint':
            if result.loc[i]['upOrDown'] == 1 and i != last:
                temp[i + 1] = 'highPoint'
                temp[i] = 'raising'
                i += 1
            elif i != 0 and result.loc[i - 1]['upOrDown'] == -1:
                temp[i - 1] = 'highPoint'
                temp[i] = 'float'
                i -= 1
            else:
                i += 1
        #Check for if there is lower point
        elif temp[i] == 'lowPoint':
            if result.loc[i]['upOrDown'] == -1 and i != last:
                temp[i + 1] = 'lowPoint'
                temp[i] = 'droping'
                i += 1
            elif i != 0 and result.loc[i - 1]['upOrDown'] == 1 :
                temp[i - 1] = 'lowPoint'
                temp[i] = 'float'
                i -= 1
            else:
                i += 1
        #Check for if initial position is not the real beginning for raise
        elif temp[i] == 'beginRaise':
            if result.loc[i]['upOrDown'] == -1 and i != last:
                temp[i + 1] = 'beginRaise'
                temp[i] = 'float'
                i += 1
            elif i != 0 and result.loc[i - 1]['upOrDown'] == 1 :
                temp[i - 1] = 'beginRaise'
                temp[i] = 'raising'
                i -= 1
            else:
                i += 1
        #Check for if initial position is not the real beginning for drop
        elif temp[i] == 'beginDrop':
            if result.loc[i]['upOrDown'] == 1 and i != last:
                temp[i + 1] = 'beginDrop'
                temp[i] = 'float'
                i += 1
            elif i != 0 and result.loc[i - 1]['upOrDown'] == -1:
                temp[i - 1] = 'beginDrop'
                temp[i] = 'droping'
                i -= 1
            else:
                i += 1
        else:
            i += 1
    result['position'] = temp


def adjustOnTime(result):
    result['upOrDown'] = result['upOrDown'].shift(1)
    result['lastDayPosition'] = result['position'].shift(1)


# ### 数据输出

#输出涨幅和跌幅的情况
def getInfoForHighLowPoint(result):
    dropdata = []
    raisedata = []
    length = result.shape[0]
    for i in result[result['position'] == 'beginRaise'].index:
        j = i
        while result.loc[j]['position'] != 'highPoint' and result.loc[j]['position'] != 'beginDrop':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] < 0:
            j += 1
        dropdata.append(result.loc[i:j])
    for i in result[result['position'] == 'beginDrop'].index:
        j = i
        while result.loc[j]['position'] != 'lowPoint' and result.loc[j]['position'] != 'beginRaise':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] > 0:
            j += 1
        raisedata.append(result.loc[i:j])
    return dropdata, raisedata

def getInfoOnRaise(data, resultRaise):
    if data[data['position'] == 'beginRaise'].shape[0] >= 1:
        beginIndex = data[data['position'] == 'beginRaise'].index[0]
        highIndex = data[(data['position'] == 'highPoint') | (data['position'] == 'beginDrop')].index[0]
        maxPrice = data['close'][highIndex]
        rateOfIncrease = maxPrice / data['close'][beginIndex] - 1
        increaseLength = highIndex - beginIndex + 1
        for index in data.index:
            if index <= highIndex:
                continue
            else:
                if data.loc[index]['upOrDown'] == 1:
                    break
                else:
                    info = {
                        'rate of increase' : rateOfIncrease,
                        'increase length' : increaseLength,
                        'rate of decrease': (data['close'][index] - maxPrice) / maxPrice,
                        'decrease length' : index - highIndex,
                        'rate of decrease today': data['change'][index],
                        'change next day' : data.loc[index + 1]['change'],
                        'bounce back point' : data.iloc[-2]['close']/data['close'][index] - 1
                    }
                    resultRaise = resultRaise.append(info, ignore_index=True)
        return resultRaise
    else:
        return resultRaise


def getInfoOnDrop(data, resultDrop):
    if data[data['position'] == 'beginDrop'].shape[0] >= 1:
        beginIndex = data[data['position'] == 'beginDrop'].index[0]
        lowIndex = data[(data['position'] == 'lowPoint') | (data['position'] == 'beginRaise')].index[0]
        minPrice = data['close'][lowIndex]
        rateOfDecrease = minPrice / data['close'][beginIndex] - 1
        decreaseLength = lowIndex - beginIndex + 1
        for index in data.index:
            if index <= lowIndex:
                continue
            else:
                if data.loc[index]['upOrDown'] == -1:
                    break
                else:
                    info = {
                        'rate of decrease' : rateOfDecrease,
                        'decrease length' : decreaseLength,
                        'rate of increase': (data['close'][index] - minPrice) / minPrice,
                        'increase length' : index - lowIndex,
                        'rate of increase today': data['change'][index],
                        'change next day' : data.loc[index + 1]['change'],
                        'bounce back point' : data.iloc[-2]['close']/data['close'][index] - 1
                    }
                    resultDrop = resultDrop.append(info, ignore_index=True)
        return resultDrop
    else:
        return resultDrop

def computation(data, upAndDown, unsureRate, con, i):
    print(i + ' start')
    Intial = data.iloc[::-1]
    mergeDay = 1
    timeSeries = Intial.copy()
    while func(timeSeries, math.ceil(upAndDown / mergeDay),
               math.ceil(upAndDown / mergeDay),
               mergeDay)[0] > unsureRate and mergeDay < int(upAndDown / 2):
        mergeDay += 1
        timeSeries = Intial.iloc[::mergeDay]
    timeSeries = func(timeSeries, math.ceil(upAndDown / mergeDay),
                      math.ceil(upAndDown / mergeDay), mergeDay)[1]
    result = giveUpOrDown(Intial).merge(
        timeSeries[['trade_date', 'Trend']],
        how='left',
        left_on='trade_date',
        right_on='trade_date').fillna(method='pad')
    givePosition(result)
    adjustOnTime(result)
    result['change'] = result['close'].pct_change()
    #result.to_sql(name = i, con = con, if_exists='replace', index = None)
    return result


def classify4class(x):
    if x >= 0.02:
        return 'Raise a lot'
    elif x > 0:
        return 'Raise a bit'
    elif x > -0.02:
        return 'Drop a bit'
    else:
        return 'Drop a lot'

def classify2class(x):
    if x >= 0:
        return 'Raise'
    else:
        return 'Drop'


def drawDecisionTree(data, name):
    data['change next day class'] = data['change next day'].apply(classify)
    X_train,X_test,y_train,y_test = train_test_split(data[['rate of increase', 'increase length', 'rate of decrease',
           'decrease length']],data['change next day class'], test_size=0.2, random_state=42)
    tree = DecisionTreeClassifier(max_depth=6,random_state=0)
    tree.fit(X_train,y_train)
    print('Train score:{:.3f}'.format(tree.score(X_train,y_train)))
    print('Test score:{:.3f}'.format(tree.score(X_test,y_test)))
    #生成可视化图
    export_graphviz(tree,out_file="tree.dot",feature_names=['rate of increase', 'increase length', 'rate of decrease',
           'decrease length'],impurity=False,filled=True)
    #展示可视化图
    graph = pydotplus.graph_from_dot_file('tree.dot')
    graph.write_pdf(name+'.pdf')

