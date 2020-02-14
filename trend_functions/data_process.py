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

import warnings
import sqlite3 as db
import math
import time

import pandas as pd
from sklearn.tree import export_graphviz
from sklearn.tree import DecisionTreeClassifier
from sklearn.model_selection import train_test_split
import pydotplus


def give_direction(stock_data):
    """
    give 1 for stock that went up, 0 for stock that didn't change, and -1 for
    stock that went down.

    Args:
        stock_data (DataFrame): The stock data we want to put action on.

    Returns:
        stock_data (DataFrame): The stock data with an extra result column.

    """

    stock_data['upOrDown'] = 1
    stock_data[stock_data['change'] == 0]['upOrDown'] = 0
    stock_data[stock_data['change'] < 0]['upOrDown'] = -1
    return stock_data


# 给定趋势并且检验合并周期是否合适
def func(stock_data, up, down, mergeDay):
    """
    Give trend signal to each day and test if the merge day is correct

    Args:
        stock_data (TYPE): DESCRIPTION.
        up (TYPE): DESCRIPTION.
        down (TYPE): DESCRIPTION.
        mergeDay (TYPE): DESCRIPTION.

    Returns:
        list: DESCRIPTION.

    """

    # warnings.filterwarnings("ignore")
    stock_data = give_direction(stock_data)
    temp = 0
    temp1 = pd.Series(index=stock_data.index).fillna(0)
    for i in stock_data.index:
        if temp >= 0:
            if stock_data.loc[i]['upOrDown'] > 0:
                temp += 1
                if temp == up:
                    for j in range(up):
                        temp1[i + j * mergeDay] = 1
                if temp >= up:
                    temp1[i] = 1
            else:
                temp = -1
        else:
            if stock_data.loc[i]['upOrDown'] < 0:
                temp -= 1
                if temp == -down:
                    for j in range(down):
                        temp1[i + j * mergeDay] = -1
                if temp <= -down:
                    temp1[i] = -1
            else:
                temp = 1
    stock_data['Trend'] = temp1
    rate = stock_data.groupby(['Trend']).size() / stock_data.shape[0]
    return [rate[0], stock_data]
    # print('up rate is ' + str(rate[1]), 'down rate is ' + str(rate[-1]), 'Unsure rate is '+ str(rate[0]))


# 定义高点/低点和趋势的起点
def givePosition(result):
    warnings.filterwarnings("ignore")
    temp = []
    # start = time.clock()
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
    # print(time.clock()-start)
    adjust(result)


# 调整保证起始点和高点/低点都在正确的日线位置上
def adjust(result):
    warnings.filterwarnings("ignore")
    i = 0
    temp = result['position']
    last = result.index[-1]
    while i <= last:
        # Check for if there is higher point
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
        # Check for if there is lower point
        elif temp[i] == 'lowPoint':
            if result.loc[i]['upOrDown'] == -1 and i != last:
                temp[i + 1] = 'lowPoint'
                temp[i] = 'droping'
                i += 1
            elif i != 0 and result.loc[i-1]['upOrDown'] == 1:
                temp[i - 1] = 'lowPoint'
                temp[i] = 'float'
                i -= 1
            else:
                i += 1
        # Check for if initial position is not the real beginning for raise
        elif temp[i] == 'beginRaise':
            if result.loc[i]['upOrDown'] == -1 and i != last:
                temp[i + 1] = 'beginRaise'
                temp[i] = 'float'
                i += 1
            elif i != 0 and result.loc[i-1]['upOrDown'] == 1:
                temp[i - 1] = 'beginRaise'
                temp[i] = 'raising'
                i -= 1
            else:
                i += 1
        # Check for if initial position is not the real beginning for drop
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


# 输出涨幅和跌幅的情况
def getInfoForHighLowPoint(result, name, con=None):
    warnings.filterwarnings("ignore")
    dropdata = []
    raisedata = []
    length = result.shape[0]
    # cleaned = pd.DataFrame(columns = result.columns)
    for i in result[result['position'] == 'beginRaise'].index:
        j = i
        while result.loc[j]['position'] != 'highPoint' and result.loc[j]['position'] != 'beginDrop':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] < 0:
            j += 1
        dropdata.append(result.loc[i:j])
        # cleaned = pd.concat([cleaned, dropdata[-1]],axis=0,ignore_index=True)
    for i in result[result['position'] == 'beginDrop'].index:
        j = i
        while result.loc[j]['position'] != 'lowPoint' and result.loc[j]['position'] != 'beginRaise':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] > 0:
            j += 1
        raisedata.append(result.loc[i:j])
        # cleaned = pd.concat([cleaned, raisedata[-1]],axis=0,ignore_index=True)
    # cleaned.to_sql(name = name, con = con, if_exists='replace', index = None)
    return dropdata, raisedata


def getInfoOnRaise(data, resultRaise):
    warnings.filterwarnings("ignore")
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
                if data.loc[index]['upOrDown'] == 1 or (index + 1) not in data.index:
                    break
                else:
                    info = {
                        'rate of increase': rateOfIncrease,
                        'increase length': increaseLength,
                        'rate of decrease': (data['close'][index] - maxPrice) / maxPrice,
                        'decrease length': index - highIndex,
                        'rate of decrease today': data['change'][index],
                        'change next day': data.loc[index + 1]['change'],
                        'bounce back point': data.iloc[-2]['close']/data['close'][index] - 1
                    }
                    resultRaise = resultRaise.append(info, ignore_index=True)
        return resultRaise
    else:
        return resultRaise


def getInfoOnDrop(data, resultDrop):
    warnings.filterwarnings("ignore")
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
                if data.loc[index]['upOrDown'] == -1 or (index + 1) not in data.index:
                    break
                else:
                    info = {
                        'rate of decrease': rateOfDecrease,
                        'decrease length': decreaseLength,
                        'rate of increase': (data['close'][index] - minPrice) / minPrice,
                        'increase length': index - lowIndex,
                        'rate of increase today': data['change'][index],
                        'change next day': data.loc[index + 1]['change'],
                        'bounce back point': data.iloc[-2]['close']/data['close'][index] - 1
                    }
                    resultDrop = resultDrop.append(info, ignore_index=True)
        return resultDrop
    else:
        return resultDrop


def computation(data, upAndDown, unsureRate, conpath, i):
    warnings.filterwarnings("ignore")
    try:
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
        result = give_direction(Intial).merge(
            timeSeries[['trade_date', 'Trend']],
            how='left',
            left_on='trade_date',
            right_on='trade_date').fillna(method='pad')
        givePosition(result)
        adjustOnTime(result)
        result['change'] = result['close'].pct_change()
        con = db.connect(conpath)
        result.to_sql(name=i, con=con, if_exists='replace', index=None)
        con.close
        return result
    except Exception as ex:
        msg = "you cuo wu, shi:%s" % ex
        print(msg)


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
    X_train, X_test, y_train, y_test = train_test_split(data[['rate of increase', 'increase length', 'rate of decrease',
           'decrease length']],data['change next day class'], test_size=0.2, random_state=42)
    tree = DecisionTreeClassifier(max_depth=6, random_state=0)
    tree.fit(X_train, y_train)
    print('Train score:{:.3f}'.format(tree.score(X_train, y_train)))
    print('Test score:{:.3f}'.format(tree.score(X_test, y_test)))
    # 生成可视化图
    export_graphviz(tree, out_file="tree.dot", feature_names=['rate of increase', 'increase length', 'rate of decrease',
           'decrease length'], impurity=False, filled=True)
    # 展示可视化图
    graph = pydotplus.graph_from_dot_file('tree.dot')
    graph.write_pdf(name+'.pdf')
