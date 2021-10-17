# -*- coding: utf-8 -*-
"""
Created on Sat Oct 16 16:39:09 2021

@author: rishi
"""

import cx_Oracle
from datetime import timedelta
import cbpro
import pandas as pd
from multiprocessing.pool import ThreadPool
dataList           = []
break_ct           = 100000

table       = ''
productId   = ''
grain       = ''
sql         = ''

def oracleConnection():
    try:
        conn = cx_Oracle.connect('****/****')
        #cur = conn.cursor()
        #print("Connection established")
        return conn
    except Exception as e:
        print("Exception occurrred")
        print(str(e))
        
##print(datetime(2015, 8, 1, 0))

def getIncrDateParam():
    global table
    global productId
    global grain
    try:
        #sql = "SELECT NVL(max(TIMESTAMP_KEY),TO_DATE(\'2015-08-01 00:00:00\',\'YYYY-MM-DD HH24:MI:SS\')) AS START_DATE,CURRENT_TIMESTAMP AS END_DATE FROM HR.COINBASE_BTC_HIST WHERE PRODUCT = \'BTC-USD\' AND GRAIN_MINUTE = 1;"
        #sql  = "SELECT NVL(max(TIMESTAMP_KEY),TO_DATE('2015-08-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS START_DATE,\
        #    CURRENT_TIMESTAMP AS END_DATE FROM HR.COINBASE_BTC_HIST WHERE PRODUCT = 'BTC-USD' AND GRAIN_MINUTE = 1"
        sql  = "SELECT NVL(max(TIMESTAMP_KEY),TO_DATE('2015-08-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS START_DATE,\
            CURRENT_TIMESTAMP AS END_DATE FROM {tableName} WHERE PRODUCT = '{productID}' AND GRAIN_MINUTE = {grain}"
        sql = sql.format(tableName=table,productID=productId,grain=grain)
        print(sql)
        conn = oracleConnection()
        c = conn.cursor()
        res = c.execute(sql)
        param = {}
        for row in res:
            #print(row)
            param['Start_Date']  = row[0]
            param['End_Date']   = row[1]
        return param
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        print("Connection and Cursor Closed")
        
#temp = getIncrDateParam('HR.COINBASE_BTC_HIST','BTC-USD',1)
#print(temp)

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def getDateParamBatch():
    global table
    global productId
    global grain
    global sql
    try:
        rng = 300
        incrDateParam = getIncrDateParam()
        #dts = [dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in datetime_range(datetime.datetime(2015, 8, 1, 0), datetime.datetime(2021, 10, 16, 18, 57, 7, 366000),timedelta(minutes=1))]
        dts = [dt.strftime('%Y-%m-%dT%H:%M:%S') for dt in datetime_range(incrDateParam['Start_Date'], incrDateParam['End_Date'],timedelta(minutes=grain))]
        param = []
        ct=0
        for row in dts:
            temp = []
            temp.append(dts[ct+1])
            temp.append(dts[ct+rng])
            ct+=rng
            param.append(temp)        
        return param            
    except Exception as e:
        temp = []
        #print("Exception Occurred in getDateParam():",str(e))
        temp.append(dts[ct+1])
        temp.append(dts[-1])
        param.append(temp)
        #print(str(e))
        return param 

def getInsertSql(tableName):
    global table
    tb = table.split(".")
    sql = "SELECT COLUMN_ID, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+tb[1]+"' ORDER BY COLUMN_ID ASC"
    try:
        conn = oracleConnection()
        c = conn.cursor()
        res = c.execute(sql)
        col1 = []
        col2 = []
        for i in res:
            col1.append(str(i[0]))
            col2.append(i[1])
        str1 = ",:".join(col1)
        str2 = ",".join(col2)
        sql = "INSERT INTO "+ table +" ("+str2+ ") VALUES (:"+str1 +")"
        return sql
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        #print("Connection and Cursor Closed")


def parallelProcessing(functionName,listData,thread):
    pool = ThreadPool(thread)
    try:
        print('Processing source data in multithread')
        result = pool.map(functionName,listData)
    except Exception as e:
        print("Exception occurred in parallelProcess")
        print(str(e))
    

def processCryptoData(tableName,product_id,grain_=1):
    global table
    global productId
    global grain
    global sql
    table       = tableName
    productId   = product_id
    grain       = grain_
    try:
        thread = 1
        param  = getDateParamBatch()
        sql    = getInsertSql(table)
        parallelProcessing(loadData,param,thread)
    except Exception as e:
        print("Exception occurred in processCryptoData")
        print(str(e))
        
def loadData(param):
    global sql
    global table
    global productId
    global grain
    try:
        cb   = cbpro.PublicClient()
        data = pd.DataFrame(cb.get_product_historic_rates(product_id=productId,start=param[0], end=param[1], granularity=60*grain))
        if len(data) > 0:            
            data.columns= ["Date","Open","High","Low","Close","Volume"]
            data['Date'] = pd.to_datetime(data['Date'], unit='s')
            #data.assign(Product=productId)
            #data.assign(Grain=grain)
            data['Product'] = productId
            data['Grain'] = grain
            #data.set_index('Date', inplace=True)
            #data.reset_index(inplace=True)
            #data.sort_values(by='Date', ascending=True, inplace=True)
            #data.reset_index(inplace=True)
            dataList = data.values.tolist()
            conn = oracleConnection()
            c = conn.cursor()
            #for i in dataList:
            for i in data.values:
                try:
                    c.execute(sql,i)
                except Exception as e:
                    print("Exception occurred while executing sql:",str(e))
                    print(i)
                    raise
        #print("Table Loaded successfully")
        return True
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        raise
        conn.rollback()
        return False
    finally:
        conn.commit()
        c.close()
        conn.close()
        #print("Connection and Cursor Closed")


def loadDataOld(dataFrame,tableName,product_id,grain_=1):
    global sql
    global table
    global productId
    global grain
    try:
        data   = dataFrame
        #sql    = getInsertSql(table)
        #print(sql)
        conn = oracleConnection()
        c = conn.cursor()
        ct = 0
        for i in data:
            ct+=1
            #print(i)
            try:
                c.execute(sql,i)
            except Exception as e:
                print("Exception occurred while executing sql:",str(e))
                print(i)
                
            if ct % 1000 == 0:
                print(str(ct),' : rows processed')
                conn.commit()
            #print(i)
        #print("Table Loaded successfully")
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        raise
        conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        #print("Connection and Cursor Closed")
        
def main():
    global dataList
    global table
    global productId
    global grain
    try:
        #grain =  [60, 300, 900, 3600, 21600, 86400]
        #processCryptoData('HR.COINBASE_BTC_HIST','BTC-USD',1)                 #processCryptoData(table,product_id,grain=1)
        #processCryptoData('HR.COINBASE_BTC_HIST','BTC-USD',5)                  #processCryptoData(table,product_id,grain=1)
        #processCryptoData('HR.COINBASE_BTC_HIST','BTC-USD',15)
        #processCryptoData('HR.COINBASE_BTC_HIST','BTC-USD',60)
        processCryptoData('HR.COINBASE_ETH_HIST','ETH-USD',1)                 #processCryptoData(table,product_id,grain=1)
        processCryptoData('HR.COINBASE_ETH_HIST','ETH-USD',5)                  #processCryptoData(table,product_id,grain=1)
        processCryptoData('HR.COINBASE_ETH_HIST','ETH-USD',15)
        processCryptoData('HR.COINBASE_ETH_HIST','ETH-USD',60)           
    except Exception as e:
        print("Exception occurred",str(e))
    
if __name__ == '__main__':
    if main():
        True
    else:
        False
    