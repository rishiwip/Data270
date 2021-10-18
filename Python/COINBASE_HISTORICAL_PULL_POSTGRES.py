# -*- coding: utf-8 -*-
"""
Created on Sat Oct 16 16:39:09 2021

@author: rishi
"""
import time
import cx_Oracle
import psycopg2 as pg
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
dataList    = []
excepList   = []
thread      = 1

def oracleConnection():
    try:
        conn = cx_Oracle.connect('***/***')
        return conn
    except Exception as e:
        print("Exception occurrred")
        print(str(e))

def postgresConnection():
    try:
        connection = pg.connect(
            host="localhost",
            user='postgres',
            password="Welcome123",
        )
        print('postgresConnection established...')
        return connection
    except Exception as e:
        print("Exception occurrred")
        print(str(e))
        
##print(datetime(2015, 8, 1, 0))

def getIncrDateParam():
    global table
    global productId
    global grain
    try:
        #sql  = "SELECT NVL(max(TIMESTAMP_KEY),TO_DATE('2015-08-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) AS START_DATE,\
        #    CURRENT_TIMESTAMP AS END_DATE FROM {tableName} WHERE PRODUCT = '{productID}' AND GRAIN_MINUTE = {grain}"
        sql = "with temp as ( select MAX(TIMESTAMP_KEY) as TIMESTAMP_KEY \
        from {tableName} where PRODUCT = '{productID}'	and GRAIN_MINUTE = {grain}) \
        select case	when T.TIMESTAMP_KEY is null \
        then to_timestamp('2015-08-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') \
        else T.TIMESTAMP_KEY end as START_DATE,\
        current_timestamp as END_DATE \
        from temp T"
        #sql = 'select current_timestamp'
        sql = sql.format(tableName=table,productID=productId,grain=grain)
        #print(sql)
        conn = postgresConnection()
        c = conn.cursor()
        c.execute(sql)
        res = c.fetchall()
        param = {}
        for row in res:
            #print(row)
            param['Start_Date']  = row[0]
            param['End_Date']   = row[1]
        print(param)
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
    #sql = "SELECT COLUMN_ID, COLUMN_NAME FROM ALL_TAB_COLUMNS WHERE TABLE_NAME = '"+tb[1]+"' ORDER BY COLUMN_ID ASC"
    sql = "select ordinal_position,column_name from information_schema.columns WHERE upper(TABLE_NAME) = '"+tb[1].upper()+"' ORDER BY ordinal_position ASC"
    #print(sql)
    try:
        conn = postgresConnection()
        c = conn.cursor()
        c.execute(sql)
        res = c.fetchall()
        col1 = []
        col2 = []
        for i in res:
            #col1.append(str(i[0]))
            col1.append('%s')
            col2.append(i[1])
        str1 = ",".join(col1)
        str2 = ",".join(col2)
        sql = "INSERT INTO "+ table +" ("+str2+ ") VALUES ("+str1 +")"
        #print(sql)
        return sql
    except Exception as e:
        print("Exception occurred ")
        print(str(e))
        #conn.rollback()
    finally:
        conn.commit()
        c.close()
        conn.close()
        #print("Connection and Cursor Closed")


def parallelProcessing(functionName,listData):
    global thread
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
    global dataList
    global excepList
    table       = tableName
    productId   = product_id
    grain       = grain_
    try:
        #thread = 5
        param  = getDateParamBatch()
        print('Prameter fetch completed')
        sql    = getInsertSql(table)
        print('Insert script generated')
        parallelProcessing(processData,param)
        print('parallelProcessing completed')
        loadDataDB(dataList)
        if len(excepList)>0:
            print('Processing exception records')
            loadDataDB(excepList)
    except Exception as e:
        print("Exception occurred in processCryptoData")
        print(str(e))


def loadDataDB(inputList):
    try:
        conn = postgresConnection()
        c = conn.cursor()
        ct = 0
        #total = 0
        for row in inputList:
            ct+=1
            c.execute(sql,row)
            if ct % 10000 == 0:
                conn.commit()
                print("Records processed into database:",str(ct))
    except Exception as e:
        print("Exception occurred in processCryptoData")
        print(str(e))        
    finally:
        conn.commit()
        c.close()
        conn.close()
        print("Connection and Cursor Closed")

def processData(param):
    global sql
    global table
    global productId
    global grain
    global dataList
    global excepList
    sourceList = []
    dataLs = []
    data = []
    status = ''
    retry = 0
    try:
        cb   = cbpro.PublicClient()
        #print('sourceList len is',len(sourceList))        
        while status != 'Ok':
            sourceList = cb.get_product_historic_rates(product_id=productId,start=param[0], end=param[1], granularity=60*grain)
            #print('sourceList len is',len(sourceList))
            if len(sourceList) > 0:
                data = pd.DataFrame(sourceList)
                data.columns= ["Date","Open","High","Low","Close","Volume"]
                data['Date'] = pd.to_datetime(data['Date'], unit='s')
                data['Product'] = productId
                data['Grain'] = str(grain)
                dataLs = data.values.tolist()
                for i in dataLs:
                    #for i in data.values:
                    dataList.append(i)
                status = 'Ok'
            else:
                retry+=1
                if retry <= 5:
                    time.sleep(5)
                    print('API did not return record, trying again in 5 seconds')
                else:
                    status = 'Ok'
        return True
    except Exception as e:
        print("Exception occurred in processData..")
        print(param)
        print(sourceList)     

def main():
    global dataList
    global table  
    global productId
    global grain
    global thread
    thread  = 2
    try:
        #grain =  [60, 300, 900, 3600, 21600, 86400]
        #processCryptoData(table,product_id,grain=1)  
        processCryptoData('cryptodb.COINBASE_BTC_HIST','BTC-USD',1)        
        processCryptoData('cryptodb.COINBASE_BTC_HIST','BTC-USD',5) 
        processCryptoData('cryptodb.COINBASE_BTC_HIST','BTC-USD',15)
        processCryptoData('cryptodb.COINBASE_BTC_HIST','BTC-USD',60)
        print("Table Loaded COINBASE_BTC_HIST")                     
        processCryptoData('cryptodb.COINBASE_ETH_HIST','ETH-USD',1) 
        processCryptoData('cryptodb.COINBASE_ETH_HIST','ETH-USD',5) 
        processCryptoData('cryptodb.COINBASE_ETH_HIST','ETH-USD',15)
        processCryptoData('cryptodb.COINBASE_ETH_HIST','ETH-USD',60)
        print("Table Loaded COINBASE_ETH_HIST")           
    except Exception as e:
        print("Exception occurred",str(e))
    
if __name__ == '__main__':
    if main():
        True
    else:
        False
    