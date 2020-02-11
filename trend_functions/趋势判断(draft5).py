
# coding: utf-8

# ## 模型定义

# 首先进行上涨，下跌和震荡的趋势判断
# 1. 定义上涨，下跌和震荡的判断条件
#     1. 上涨：连续上涨10天
#     2. 下跌：连续下跌10天
#     3. 震荡：当这段时间既不属于上涨又不属于下跌时
# 2. 选择合适的合并时段以平滑曲线
#     1. 从日线开始，判断上涨和下跌的比例，如比例过低则改用2日线、3日线以此类推
#     2. 目标让震荡区间小于70%
#     3. 不采用移动均线因移动均线由于其定义，拐点必在价格拐点后，故现实意义不强
# 

# In[1]:

import numpy as np
import pandas as pd
import datetime
import tushare as ts
from datetime import datetime
from datetime import timedelta
import sqlite3 as db
import math
import time


# In[2]:

import warnings
warnings.filterwarnings("ignore")


# ## 导入数据，确定参数

# In[3]:

#导入数据

def getData():
    con = db.connect('data.sqlite')
    #con = db.connect('C:\\Users\\hp\\OneDrive - pku.edu.cn\\实习\\华宝\\PE-ROE Project\\code\\data.sqlite')
    cur = con.cursor()
    data = {}
    cur.execute("select name from sqlite_master where type='table'")
    tab_name = cur.fetchall()
    tab_name = [line[0] for line in tab_name]
    for i in tab_name:
        data[i] = pd.read_sql_query("SELECT trade_date, close FROM '" + i + "'",
                                    con)
        '''
        data[i] = data[i][data[i]['trade_date'] > '20090101']
        '''
        if data[i].shape[0] < 100:
            data.pop(i)
    cur.close()
    con.close()
    return data



# In[ ]:
'''
def getData():
    stockData = {}
    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    stockList = data['ts_code']
    count = 0
    for i in stockList:
        count+= 1
        if count % 300 == 0:
            time.sleep(60)
        #由于每次最多只能得到4000条数据，故最多只到2002年的数据
        startDate = '20090101'
        endDate = '20190701'

        stockData[i] = pro.daily(ts_code=i, start_date=startDate, end_date=endDate)[[
            'trade_date', 'open', 'high', 'low', 'close', 'amount'
        ]]
        if stockData[i].shape[0] < 100:
            stockData.pop(i)
        
    return stockData
'''

# ## 定义函数

# ### 数据处理

# In[4]:

#set up or down for each day
def giveUpOrDown(timeSeries):
    timeSeries['upOrDown'] = timeSeries['close'] - timeSeries['close'].shift(
        -1) < 0
    timeSeries['upOrDown'] = timeSeries['upOrDown'].astype("int")
    timeSeries['upOrDown'][timeSeries['upOrDown'] == 0] = -1
    return timeSeries


# In[5]:

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


# In[6]:

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


# In[7]:

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


# In[8]:

def adjustOnTime(result):
    result['upOrDown'] = result['upOrDown'].shift(1)
    result['lastDayPosition'] = result['position'].shift(1)


# ### 数据输出

# In[9]:

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


# In[10]:

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
                elif (index+1) not in data.index:
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


# In[22]:

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
                elif (index+1) not in data.index:
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


# ## 主程序

# In[20]:

def computation(data, upAndDown, unsureRate, con, i):
    print(i+' start')
    Intial = data.iloc[::-1]
    mergeDay = 1
    timeSeries = Intial.copy()
    while func(timeSeries, math.ceil(upAndDown / mergeDay),
               math.ceil(upAndDown / mergeDay), mergeDay)[0] > unsureRate:
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
    result.to_sql(name = i, con = con, if_exists='replace', index = None)
    return result


# In[23]:

if __name__=='__main__':
    resultRaise = pd.DataFrame(columns=[
        'rate of increase', 'increase length', 'rate of decrease',
        'decrease length', 'rate of decrease today', 'change next day', 'bounce back point'
        ])
    resultDrop = pd.DataFrame(columns=[
        'rate of decrease', 'decrease length', 'rate of increase',
        'increase length', 'rate of increase today', 'change next day', 'bounce back point'
        ])
    data = getData()
    upAndDown = 9
    unsureRate = 0.75
    dropdata = {}
    raisedata = {}
    con = db.connect('data-up-and-down9.sqlite')
    #con = db.connect('C:\\Users\\hp\\OneDrive - pku.edu.cn\\实习\\华宝\\PE-ROE Project\\code\\data.sqlite')
    cur = con.cursor()
    #p = Pool(10)
    for i in data:
        result = computation(data[i], upAndDown, unsureRate, con, i)
        #p.apply_async(computation, args=(data[i], upAndDown, unsureRate, con, i, ))
        dropdata[i], raisedata[i] = getInfoForHighLowPoint(result)
        for j in dropdata[i]:
            resultRaise = getInfoOnRaise(j, resultRaise)
        for j in raisedata[i]:
            resultDrop = getInfoOnDrop(j, resultDrop)
    con.close()
    cur.close()


# In[24]:

resultRaise.to_csv('rasie.csv')
resultDrop.to_csv('drop.csv')


# In[ ]:



