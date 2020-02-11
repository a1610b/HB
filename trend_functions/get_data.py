import numpy as np
import pandas as pd
import datetime
import tushare as ts
from datetime import datetime
import sqlite3 as db
import time

def getDataFromsql(item = '*', name = 'stock_data'):
    con = db.connect('D:\\Data\\'+ name + '.sqlite')
    #con = db.connect('C:\\Users\\hp\\OneDrive - pku.edu.cn\\实习\\华宝\\PE-ROE Project\\code\\data.sqlite')
    cur = con.cursor()
    data = {}
    cur.execute("select name from sqlite_master where type='table'")
    tab_name = cur.fetchall()
    tab_name = [line[0] for line in tab_name]
    for i in tab_name:
        data[i] = pd.read_sql_query("SELECT " + item + " FROM '" + i + "'",
                                    con)
        data[i] = data[i][data[i]['trade_date'] > '20090101']
        if data[i].shape[0] < 100:
            data.pop(i)
    cur.close()
    con.close()
    return data

def get_industry_stock_list():
    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()
    df = pro.index_classify(level='L1', src='SW')['index code']
    industry_stockList ={}
    for i in df:
        industry_stockList[i] = pro.index_member(index_code= i)
    return industry_stockList

#从本地csv导入数据
#csv中只有一列为价格，价格应为从新到旧
def getDataFromeCSV():
    filename = input('filename: ')
    data = {}
    data['first'] = pd.read_csv(filename)
    data['first'].columns = ['close']
    data['first']['trade_date'] = pd.date_range(start='2019-1-09',
        periods=data['first'].shape[0], freq='-1D')
    return data

def download_all_market_data(sqlname = 'data'):
    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()

    #从数据库有数据开始到当前日期
    endDate = time.strftime("%Y%m%d", time.localtime())

    #读取全市场的股票信息
    #读取全市场的股票信息

    stockList = set(pro.stock_basic(exchange='', list_status='D', 
                                    fields='ts_code,symbol,name,area,industry,list_date')['ts_code']) | \
                set(pro.stock_basic(exchange='', list_status='L', 
                                    fields='ts_code,symbol,name,area,industry,list_date')['ts_code']) | \
                set(pro.stock_basic(exchange='', list_status='P', 
                                    fields='ts_code,symbol,name,area,industry,list_date')['ts_code'])

    con = db.connect('D:\\Data\\'+sqlname+'.sqlite')
    cur = con.cursor()

    count = 0
    for i in stockList:
        #避免过于频繁的调用爬虫
        '''
        count += 1
        if count % 100 == 0:
            time.sleep(60)
        '''
        
        df1 = pro.daily(ts_code=i)           #基本数据
        df2 = pro.daily_basic(ts_code=i)     #基本面数据
        df3 = pro.adj_factor(ts_code=i)      #复权因子
        df4 = pro.moneyflow(ts_code=i)       #资金流向
        df5 = pro.margin_detail(ts_code=i)   #融资融券

        stockData = df1[set(df1.columns) - {'ts_code'}].merge(df2[set(df2.columns) - {'ts_code', 'close'}], how = 'left', right_on='trade_date', left_on='trade_date')
        stockData = stockData.merge(df3[['trade_date', 'adj_factor']], how = 'left', right_on='trade_date', left_on='trade_date')
        stockData = stockData.merge(df4[set(df4.columns) - {'ts_code'}], how = 'left', right_on='trade_date', left_on='trade_date')
        stockData = stockData.merge(df5[set(df5.columns) - {'ts_code'}], how = 'left', right_on='trade_date', left_on='trade_date')
        stockData.dropna(how = 'all', axis = 1, inplace= True)
        print(i)
        #写入数据库
        stockData.to_sql(name = i, con = con, if_exists='replace', index = None)
        con.commit()
    cur.close()
    con.close()

'''
def get_index_data(index, sqlname = 'data', date = time.strftime("%Y%m%d", time.localtime())):
    ts.set_token('267addf63a14adcfc98067fc253fbd72a728461706acf9474c0dae29')
    pro = ts.pro_api()

    #从2009年1月1日开始到当前日期
    startDate = '20090101'
    endDate = time.strftime("%Y%m%d", time.localtime())

    stockList = set(pro.index_weight(
        index_code=index,
        start_date=str(int(date)-10000),
        end_date=date))

    con = db.connect('data\\'+sqlname+'.sqlite')
    cur = con.cursor()
    count = 0
    for i in stockList:
        #避免过多调用函数
        count += 1
        if count % 100 == 0:
            time.sleep(60)

        df1 = pro.daily(ts_code=i, start_date=startDate, end_date=endDate)[[
            'trade_date', 'open', 'high', 'low', 'close', 'amount'
        ]]
        df2 = pro.daily_basic(ts_code=i,
                                  start_date=startDate,
                                  end_date=endDate,
                                  fields='trade_date, pe_ttm, pb, turnover_rate, turnover_rate_f, ps_ttm')
        stockData = df1.merge(df2, right_on='trade_date', left_on='trade_date')
        print(i)
        stockData.to_sql(name = i, con = con, if_exists='replace', index = None)
        con.commit()
    cur.close()
    con.close()
'''

def main():
    download_all_market_data()
    print('done')

if __name__ == '__main__':
    main()