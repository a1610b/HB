import numpy as np
import pandas as pd
import datetime
import sqlite3 as db
from datetime import datetime
from datetime import timedelta
import sqlite3 as db
import math
import time
from sklearn.tree import export_graphviz
from sklearn.tree import DecisionTreeClassifier
from sklearn import tree
from sklearn.model_selection import train_test_split
import pydotplus
import warnings

#set up or down for each day
def giveUpOrDown(timeSeries):
    timeSeries['upOrDown'] = timeSeries['close'] - timeSeries['close'].shift(
        -1) < 0
    timeSeries['upOrDown'] = timeSeries['upOrDown'].astype("int")
    timeSeries['upOrDown'][timeSeries['upOrDown'] == 0] = -1
    return timeSeries

#给定趋势并且检验合并周期是否合适
def func(timeSeries, Up, Down, mergeDay):
    warnings.filterwarnings("ignore")
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
    warnings.filterwarnings("ignore")
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
    warnings.filterwarnings("ignore")
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


#输出涨幅和跌幅的情况
def getInfoForHighLowPoint(result, name, con = None):
    warnings.filterwarnings("ignore")
    dropdata = []
    raisedata = []
    length = result.shape[0]
    #cleaned = pd.DataFrame(columns = result.columns)
    for i in result[result['position'] == 'beginRaise'].index:
        j = i
        while result.loc[j]['position'] != 'highPoint' and result.loc[j]['position'] != 'beginDrop':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] < 0:
            j += 1
        dropdata.append(result.loc[i:j])
        #cleaned = pd.concat([cleaned, dropdata[-1]],axis=0,ignore_index=True)
    for i in result[result['position'] == 'beginDrop'].index:
        j = i
        while result.loc[j]['position'] != 'lowPoint' and result.loc[j]['position'] != 'beginRaise':
            j += 1
        j += 1
        while j < length and result.loc[j]['upOrDown'] > 0:
            j += 1
        raisedata.append(result.loc[i:j])
        #cleaned = pd.concat([cleaned, raisedata[-1]],axis=0,ignore_index=True)
    #cleaned.to_sql(name = name, con = con, if_exists='replace', index = None)
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
        result = giveUpOrDown(Intial).merge(
            timeSeries[['trade_date', 'Trend']],
            how='left',
            left_on='trade_date',
            right_on='trade_date').fillna(method='pad')
        givePosition(result)
        adjustOnTime(result)
        result['change'] = result['close'].pct_change()
        con = db.connect(conpath)
        result.to_sql(name = i, con = con, if_exists='replace', index = None)
        con.close
        return result
    except Exception as ex:
        msg = "you cuo wu, shi:%s"%ex
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


