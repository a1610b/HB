# -*- coding: utf-8 -*-
"""
Created on Wed Jul 17 10:17:42 2019

@author: shankuanzhi
"""

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


# In[50]:

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


# In[51]:

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


con = db.connect('data-up-and-down.sqlite')
cur = con.cursor()
data = {}
cur.execute("select name from sqlite_master where type='table'")
tab_name = cur.fetchall()
tab_name = [line[0] for line in tab_name]
for i in tab_name:
    data[i] = pd.read_sql_query("SELECT trade_date, close FROM '" + i + "'",
                                con)
cur.close()
con.close()


dropdata[i], raisedata[i] = getInfoForHighLowPoint(result)
for j in dropdata[i]:
    resultRaise = getInfoOnRaise(j, resultRaise)
for j in raisedata[i]:
    resultDrop = getInfoOnDrop(j, resultDrop)
    
#输出涨幅和跌幅的情况
resultRaise.to_csv('rasie2.csv')
resultDrop.to_csv('drop2.csv')
