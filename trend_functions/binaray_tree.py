# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 17:31:04 2019

@author: shankuanzhi
"""

def scoreresult(data):
    predict = data[0]
    actual = data[1]
    if predict == 'Raise a lot':
        if actual == 'Raise a lot':
            return 1
        elif actual == 'Raise a bit':
            return 0.5
        elif actual == 'Drop a bit':
            return -0.5
        elif actual =='Drop a lot':
            return -1
        else:
            print('Error')
    elif predict == 'Raise a bit':
        if actual == 'Raise a lot':
            return 0.5
        elif actual == 'Raise a bit':
            return 0.25
        elif actual == 'Drop a bit':
            return -0.25
        elif actual =='Drop a lot':
            return -0.5
        else:
            print('Error')
    elif predict == 'Drop a bit':
        if actual == 'Raise a lot':
            return -0.5
        elif actual == 'Raise a bit':
            return -0.25
        elif actual == 'Drop a bit':
            return 0.25
        elif actual =='Drop a lot':
            return 0.5
        else:
            print('Error')
    elif predict == 'Drop a lot':
        if actual == 'Raise a lot':
            return -1
        elif actual == 'Raise a bit':
            return -0.5
        elif actual == 'Drop a bit':
            return 0.5
        elif actual =='Drop a lot':
            return 1
        else:
            print('Error')
    else:
        print('Error')
    
def predictByScore(data):
    DropALot = data[0]*scoreresult(['Drop a lot', 'Drop a bit'])+data[1]*scoreresult(['Drop a lot', 'Drop a lot'])+ data[2]*scoreresult(['Drop a lot', 'Raise a bit'])+ data[3]*scoreresult(['Drop a lot', 'Raise a lot'])
    DropABit = data[0]*scoreresult(['Drop a bit', 'Drop a bit'])+data[1]*scoreresult(['Drop a bit', 'Drop a lot'])+ data[2]*scoreresult(['Drop a bit', 'Raise a bit'])+ data[3]*scoreresult(['Drop a bit', 'Raise a lot'])
    RaiseALot = data[0]*scoreresult(['Raise a lot', 'Drop a bit'])+data[1]*scoreresult(['Raise a lot', 'Drop a lot'])+ data[2]*scoreresult(['Raise a lot', 'Raise a bit'])+ data[3]*scoreresult(['Raise a lot', 'Raise a lot'])
    RaiseABit = data[0]*scoreresult(['Raise a bit', 'Drop a bit'])+data[1]*scoreresult(['Raise a bit', 'Drop a lot'])+ data[2]*scoreresult(['Raise a bit', 'Raise a bit'])+ data[3]*scoreresult(['Raise a bit', 'Raise a lot'])
    if max(DropALot, DropABit, RaiseALot, RaiseABit) == DropALot:
        return 'Drop a lot'
    elif max(DropALot, DropABit, RaiseALot, RaiseABit) == DropABit:
        return 'Drop a bit'
    elif max(DropALot, DropABit, RaiseALot, RaiseABit) == RaiseALot:
        return 'Raise a lot'
    elif max(DropALot, DropABit, RaiseALot, RaiseABit) == RaiseABit:
        return 'Raise a bit'
    else:
        print('Error')

def DecisionTreeScore(data):
    data['change next day class'] = data['change next day'].apply(classify4class)
    data['decrease/increase'] = -data['rate of decrease'] / data['rate of increase']
    X_train,X_test,y_train,y_test = train_test_split(data[['decrease/increase', 'rate of increase', 'increase length', 'rate of decrease',
           'decrease length']],data['change next day class'], test_size=0.2, random_state=42)
    tree = DecisionTreeClassifier(max_depth=4,random_state=0)
    tree.fit(X_train,y_train)
    prediction = pd.DataFrame(pd.DataFrame(tree.predict_proba(X_test)).apply(predictByScore, axis = 1))
    #prediction = pd.DataFrame(tree.predict(X_test))
    result = prediction.merge(pd.DataFrame(y_test), right_index = True, left_index = True)
    return result.apply(scoreresult, axis = 1).sum(), result.apply(scoreresult, axis = 1).sum()/result.shape[0]
    
def industry_decision_tree(dropdata, raisedata):
    industry_stockList = get_data.get_industry_stock_list()
    for industry in industry_stockList:
        resultRaise = pd.DataFrame(columns=[
            'rate of increase', 'increase length', 'rate of decrease',
            'decrease length', 'rate of decrease today', 'change next day', 'bounce back point'
            ])
        resultDrop = pd.DataFrame(columns=[
            'rate of decrease', 'decrease length', 'rate of increase',
            'increase length', 'rate of increase today', 'change next day', 'bounce back point'
            ])
        for stock in industry_stockList[industry]:
            if stock in data.keys():
                dropdata[stock], raisedata[stock] = data_process.getInfoForHighLowPoint(data[stock])
                for j in dropdata[stock]:
                    resultRaise = data_process.getInfoOnRaise(j, resultRaise)
                for j in raisedata[stock]:
                    resultDrop = data_process.getInfoOnDrop(j, resultDrop)
        print('*'*50)
        print(industry)
        print("回撤结果：")
        print(DecisionTreeScore(resultRaise))
        print("\n反弹结果：")
        print(DecisionTreeScore(resultDrop))  
