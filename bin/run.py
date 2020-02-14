import trend.data_process as dp
import trend.get_data as gd
import pandas as pd
import sqlite3 as db

def main():
    data = gd.getDataFromsql()
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
    #p = Pool(10)
    for i in data:
        result = dp.computation(data[i], upAndDown, unsureRate, con, i)
        #p.apply_async(computation, args=(data[i], upAndDown, unsureRate, con, i, ))
        dropdata[i], raisedata[i] = dp.getInfoForHighLowPoint(result)
        for j in dropdata[i]:
            resultRaise = dp.getInfoOnRaise(j, resultRaise)
        for j in raisedata[i]:
            resultDrop = dp.getInfoOnDrop(j, resultDrop)
    con.close()
    resultRaise.to_csv('rasie2.csv')
    resultDrop.to_csv('drop2.csv')

if __name__  == '__main__':
    main()