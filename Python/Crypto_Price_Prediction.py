

import time
import cx_Oracle
import psycopg2 as pg
from datetime import timedelta
import cbpro
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from multiprocessing.pool import ThreadPool
from matplotlib.pyplot import figure
# Technical Indicators
import talib as ta

# Plotting graphs
import matplotlib.pyplot as plt

# Machine learning
from sklearn.preprocessing import PolynomialFeatures
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn import metrics
from sklearn.model_selection import cross_val_score
from sklearn.metrics import confusion_matrix
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split
from sklearn.linear_model import Ridge
from sklearn.linear_model import Lasso
from sklearn.linear_model import ElasticNet

from sklearn.neighbors import KNeighborsClassifier 
from sklearn import neighbors
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics import accuracy_score
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow.python.client import device_lib

#importing LSTM required libraries
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, Dropout, LSTM


x_train = []
y_train = []
x_test  = []
y_test  = []
train   = []
test    = []
dFrame  = []
former  = 0
def postgresConnection():
    try:
        connection = pg.connect(
            host="localhost",
            user='postgres',
            password="***",
        )
        print('postgresConnection established...')
        return connection
    except Exception as e:
        print("Exception occurrred")
        print(str(e))
        
def fetchData(sql):    
    try:
        conn = postgresConnection()
        curr = conn.cursor()
        curr.execute(sql)
        row = curr.fetchall()
        colHeader = ['year','month','week','day','date','timestamp_key','open','high','low','close','volume','product','grain_minute']
        #colHeader = ['timestamp_key','open','high','low','close']
        df = pd.DataFrame(row, columns=colHeader)
        return df
    except Exception as e:
        print("Exception occurred in processCryptoData")
        print(str(e))
    finally:
        #conn.commit()
        curr.close()
        conn.close()
        print("Connection and Cursor Closed")
        
def performLogisticRegression(dfAll):

    cols = ['timestamp_key','open','high','low','close']        
    df = dfAll[cols]
    df.sort_values(by=['timestamp_key'],ascending=True)        
    df['row_num'] = np.arange(len(df))
    df = df.drop('timestamp_key', axis=1)
    
    #Define Predictor/Independent Variables
    df['S_10'] = df['close'].rolling(window=10).mean()
    df['Corr'] = df['close'].rolling(window=10).corr(df['S_10'])
    df['RSI'] = ta.RSI(np.array(df['close']), timeperiod =10)
    df['Open-Close'] = df['open'] - df['close'].shift(1)
    df['Open-Open'] = df['open'] - df['open'].shift(1)
    #df.head()
    df = df.dropna()
    #df.head()
    X = df.iloc[:,:9]
    #X.head()
    #print('Feature Data Frame is:',X.head())
    
    #Define Target/Dependent Variable
    y = np.where(df['close'].shift(-1) > df['close'],1,0)
    #y = np.where(df['close'].shift(-1) > df['close']*1.01,1,0)
    #print('Label array is:',y)

    # splitting into train and testation

    former = len(df) * .80
    former = math.floor(former)
    later = len(df) - former
    
    train = df[:former]
    test = df[former:]
    
    # shapes of training set
    #print('\n Shape of training set:')
    #print(train.shape)
    
    # shapes of testation set
    #print('\n Shape of testation set:')
    #print(test.shape)        
    
    X_train = train
    y_train = y[:former]
    x_test = test
    y_test = y[former:]
    
    #Instantiate The Logistic Regression in Python
    model = LogisticRegression()
    model = model.fit (X_train,y_train)
    
    #Examine The Coefficients
    print('Coefficients are:',pd.DataFrame(zip(X.columns, np.transpose(model.coef_))))
    
    #Calculate Class Probabilities
    probability = model.predict_proba(x_test)
    #print('Class Probabilities are :\n',probability)
    
    #Predict Class Labels
    probability = model.predict_proba(x_test)
    #print(probability)
    predicted = model.predict(x_test)
    print('Class Lables are :',predicted)
        
    # Make the confusion matrix
    plt.clf()
    cf_matrix = metrics.confusion_matrix(y_test, predicted)
    #cf_matrix = confusion_matrix(y_true, y_pred)
    print("\nTest confusion_matrix")
    sns.heatmap(cf_matrix, annot=True, cmap='Blues')
    plt.xlabel('Predicted', fontsize=12)
    plt.ylabel('True', fontsize=12)
    

    #Evaluate The Model
    #Confusion Matrix
    print(metrics.confusion_matrix(y_test, predicted))
    
    #Classification Report
    print('Classification Report',metrics.classification_report(y_test, predicted))
    
    #Model Accuracy
    print('Model Accuracy',model.score(x_test,y_test))
    #print("Train accuracy:", np.round(accuracy_score(y_train,model.predict(x_train)), 2))
    #print("Test accuracy:", np.round(accuracy_score(y_test, model.predict(x_test)), 2))
    
    #Cross-testation
    cross_val = cross_val_score(LogisticRegression(), X, y, scoring='accuracy', cv=10)
    print('Cross-testation',cross_val)
    print('Cross-testation mean',cross_val.mean())

def spliDataSet(dataframe):    
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    # splitting into train and testation
    former = len(dataframe) * .80
    former = math.floor(former)
    later = len(dataframe) - former
    
    train = dataframe[:former]
    test = dataframe[former:]
    
    # shapes of training set
    print('\n Shape of training set:')
    print(train.shape)
    
    # shapes of testation set
    print('\n Shape of test set:')
    print(test.shape)
    #print(df)
    
    
    x_train = train.drop('close', axis=1)
    y_train = train['close']
    x_test = test.drop('close', axis=1)
    y_test = test['close']
    
def calculateStats(model):
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    global dFrame
    global former
    
    former = len(dFrame) * .80
    former = math.floor(former)
    later = len(dFrame) - former
    
    #make predictions and find the rmse
    preds = model.predict(x_test)
    rms=np.sqrt(np.mean(np.power((np.array(y_test)-np.array(preds)),2)))
    print('rms is:',rms)
    
    #Examine The Coefficients
    print('Coefficients are:',pd.DataFrame(zip(dFrame.columns, np.transpose(model.coef_))))
    
    #Predict Class Labels
    predicted = model.predict(x_test)
    print('Class Lables are :',predicted)
    
    #Evaluate The Model
    #Confusion Matrix
    #print(metrics.confusion_matrix(y_test, predicted))
    
    #Classification Report
    #print('Classification Report',metrics.classification_report(y_test, predicted))
    
    #Model Accuracy
    print('Model Accuracy',model.score(x_test,y_test))
    
    #Cross-testation
    #cross_val = cross_val_score(LinearRegression(), X, y, scoring='accuracy', cv=10)
    #print('Cross-testation',cross_val)
    #print('Cross-testation mean',cross_val.mean())
    
    #plot
    test['Predictions'] = 0
    test['Predictions'] = preds
        
    figure(figsize=(14, 6), dpi=80)
    
    plt.plot(train['close'])
    plt.plot(test[['close', 'Predictions']])

def performLinearRegression(dfAll):
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    global dFrame
    global former
    cols = ['timestamp_key','close']        
    df = dfAll[cols]
    df.sort_values(by=['timestamp_key'],ascending=True)        
    df['row_num'] = np.arange(len(df))
    dFrame = df.drop('timestamp_key', axis=1)
    print(dFrame)
    
    
    #splitting datframe betwen  train and test
    spliDataSet(dFrame)
    
    #implement linear regression
    #model = LinearRegression()
    #print('Applying model')
    #model.fit(x_train,y_train)
    
    #create polynomial features, and then train a linear regression model.
    steps = [
        ('scalar', StandardScaler()),
        ('poly', PolynomialFeatures(degree=1)),
        #('model', LinearRegression())
        ('model', Lasso(alpha=0.1, fit_intercept=True))
        #('model', LinearRegression())
    ]

    # Train the model
    model = Pipeline(steps)
    model.fit(x_train, y_train)
    
    #calculateStats
    #print('calculateStats')
    #calculateStats(model)
    
    #make predictions and find the rmse
    preds = model.predict(x_test)
    rms=np.sqrt(np.mean(np.power((np.array(y_test)-np.array(preds)),2)))
    print('rms is :' + str(rms))

    #Examine The Coefficients
    #print('Coefficients are:',pd.DataFrame(zip(dFrame.columns, np.transpose(model.coef_))))
    
    #Predict Class Labels
    predicted = model.predict(x_test)
    print('Class Lables are :',predicted)
    
    #plot
    test['Predictions'] = 0
    test['Predictions'] = preds
    figure(figsize=(14, 6), dpi=80)
    plt.plot(train['close'])
    plt.plot(test[['close', 'Predictions']])
    
def performSVM(dfAll):
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    global dFrame
    global former
    
    cols = ['timestamp_key','open','high','low','close']        
    df = dfAll[cols]
    df.sort_values(by=['timestamp_key'],ascending=True)        
    df['row_num'] = np.arange(len(df))
    df = df.drop('timestamp_key', axis=1)
    
    #Define Predictor/Independent Variables
    df['S_10'] = df['close'].rolling(window=10).mean()
    df['Corr'] = df['close'].rolling(window=10).corr(df['S_10'])
    df['RSI'] = ta.RSI(np.array(df['close']), timeperiod =10)
    df['Open-Close'] = df['open'] - df['close'].shift(1)
    df['Open-Open'] = df['open'] - df['open'].shift(1)
    #df.head()
    df = df.dropna()
    #df.head()
    X = df.iloc[:,:9]
    #X.head()
    #print('Feature Data Frame is:',X.head())
    
    #Define Target/Dependent Variable
    y = np.where(df['close'].shift(-1) > df['close'],1,0)
    
    # splitting into train and testation

    former = len(df) * .80
    former = math.floor(former)
    later = len(df) - former
    
    train = df[:former]
    test = df[former:]
    
    # shapes of training set
    #print('\n Shape of training set:')
    #print(train.shape)
    
    # shapes of testation set
    #print('\n Shape of testation set:')
    #print(test.shape)        
    
    x_train = train
    y_train = y[:former]
    x_test = test
    y_test = y[former:]

    #Instantiate The Logistic Regression in Python
    print('Applying SVM')
    model = SVC(C = 1e5, kernel = 'linear')
    model.fit (x_train,y_train)
    
    #Examine The Coefficients
    print('Coefficients are:',pd.DataFrame(zip(X.columns, np.transpose(model.coef_))))    
    
    #Predict Class Labels
    predicted = model.predict(x_test)
    print('Class Lables are :',predicted)
    
    
    # Make the confusion matrix
    plt.clf()
    cf_matrix = metrics.confusion_matrix(y_test, predicted)
    #cf_matrix = confusion_matrix(y_true, y_pred)
    print("\nTest confusion_matrix")
    sns.heatmap(cf_matrix, annot=True, cmap='Blues')
    plt.xlabel('Predicted', fontsize=12)
    plt.ylabel('True', fontsize=12)
    

    #Evaluate The Model
    #Confusion Matrix
    print(metrics.confusion_matrix(y_test, predicted))
    
    #Classification Report
    print('Classification Report',metrics.classification_report(y_test, predicted))
    
    #Model Accuracy
    print('Model Accuracy',model.score(x_test,y_test))
    
    #Cross-testation
    cross_val = cross_val_score(LogisticRegression(), X, y, scoring='accuracy', cv=10)
    print('Cross-testation',cross_val)
    print('Cross-testation mean',cross_val.mean())
    
def performKNN(dfAll):
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    global dFrame
    global former
    
    cols = ['timestamp_key','open','high','low','close']        
    df = dfAll[cols]
    df.sort_values(by=['timestamp_key'],ascending=True)        
    df['row_num'] = np.arange(len(df))
    df = df.drop('timestamp_key', axis=1)
    
    #Define Predictor/Independent Variables
    df['S_10'] = df['close'].rolling(window=10).mean()
    df['Corr'] = df['close'].rolling(window=10).corr(df['S_10'])
    df['RSI'] = ta.RSI(np.array(df['close']), timeperiod =10)
    df['Open-Close'] = df['open'] - df['close'].shift(1)
    df['Open-Open'] = df['open'] - df['open'].shift(1)
    #df.head()
    df = df.dropna()
    #df.head()
    X = df.iloc[:,:9]
    #X.head()
    #print('Feature Data Frame is:',X.head())
    
    #Define Target/Dependent Variable
    y = np.where(df['close'].shift(-1) > df['close'],1,0)
    
    # splitting into train and testation

    former = len(df) * .80
    former = math.floor(former)
    later = len(df) - former
    
    train = df[:former]
    test = df[former:]
    
    # shapes of training set
    #print('\n Shape of training set:')
    #print(train.shape)
    
    # shapes of testation set
    #print('\n Shape of testation set:')
    #print(test.shape)        
    
    x_train = train
    y_train = y[:former]
    x_test = test
    y_test = y[former:]
    
    #scaling data
    scaler = MinMaxScaler(feature_range=(0, 1))
    x_train_scaled = scaler.fit_transform(x_train)
    x_train = pd.DataFrame(x_train_scaled)
    x_valid_scaled = scaler.fit_transform(x_test)
    x_test = pd.DataFrame(x_valid_scaled)
    
    #fit the model and make predictions
    clf = KNeighborsClassifier(n_neighbors=3) 
    model = clf.fit(x_train, y_train) 

    #Predict Class Labels
    predicted = model.predict(x_test)
    print('Class Lables are :',predicted)

    ## Make the confusion matrix
    #plt.clf()
    cf_matrix = metrics.confusion_matrix(y_test, predicted)
    print("\nTest confusion_matrix")
    sns.heatmap(cf_matrix, annot=True, cmap='Blues')
    plt.xlabel('Predicted', fontsize=12)
    plt.ylabel('True', fontsize=12)

    #Evaluate The Model
    #Confusion Matrix
    print(metrics.confusion_matrix(y_test, predicted))
    #
    #Classification Report
    print('Classification Report',metrics.classification_report(y_test, predicted))
    #
    #Model Accuracy
    print('Model Accuracy',model.score(x_test,y_test))
    print("Train accuracy:", np.round(accuracy_score(y_train,clf.predict(x_train)), 2))
    print("Test accuracy:", np.round(accuracy_score(y_test, clf.predict(x_test)), 2))

    #Cross-testation
    cross_val = cross_val_score(LogisticRegression(), X, y, scoring='accuracy', cv=10)
    print('Cross-testing',cross_val)
    print('Cross-testing mean',cross_val.mean())
    
def performLSTM(dfAll):
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    global dFrame
    global former
    
    #tf.config.list_physical_devices('GPU')
    #device_lib.list_local_devices()
    
    cols = ['timestamp_key','close']        
    df = dfAll[cols]
    df.sort_values(by=['timestamp_key'],ascending=True)
    
    #df['row_num'] = np.arange(len(df))
    #dFrame = df.drop('timestamp_key', axis=1)
    #df.head()
    #print(df)
    
    #setting index
    df.index = df.timestamp_key
    df.drop('timestamp_key', axis=1, inplace=True)
    #splitting datframe betwen  train and test
    #spliDataSet(dFrame)
    
    dataset = df.values
    #print('dataset is',dataset)
    
    # splitting into train and validation

    former = len(df) * .80
    former = math.floor(former)
    later = len(df) - former

    train = df[:former]
    test = df[former:]

    # shapes of training set
    print('\n Shape of training set:')
    print(train.shape)

    # shapes of validation set
    print('\n Shape of validation set:')
    print(test.shape)


    x_train = train.drop('close', axis=1)
    y_train = train['close']
    x_test  = test.drop('close', axis=1)
    y_test  = test['close']
    
    #converting dataset into x_train and y_train
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(dataset)
    
    x_train, y_train = [], []
    for i in range(60,len(train)):
        x_train.append(scaled_data[i-60:i,0])
        y_train.append(scaled_data[i,0])
    x_train, y_train = np.array(x_train), np.array(y_train)
    
    x_train = np.reshape(x_train, (x_train.shape[0],x_train.shape[1],1))
    
    #converting dataset into x_train and y_train
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(dataset)
    
    x_train, y_train = [], []
    for i in range(60,len(train)):
        x_train.append(scaled_data[i-60:i,0])
        y_train.append(scaled_data[i,0])
    x_train, y_train = np.array(x_train), np.array(y_train)
    
    x_train = np.reshape(x_train, (x_train.shape[0],x_train.shape[1],1))
    
    # create and fit the LSTM network
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=(x_train.shape[1],1)))
    model.add(LSTM(units=50))
    model.add(Dense(1))
    
    model.compile(loss='mean_squared_error', optimizer='adam')
    model.fit(x_train, y_train, epochs=1, batch_size=1, verbose=2)
    
    #predicting 246 values, using past 60 from the train data
    inputs = df[len(df) - len(test) - 60:].values
    inputs = inputs.reshape(-1,1)
    inputs = scaler.transform(inputs)
    
    X_test = []
    for i in range(60,inputs.shape[0]):
        X_test.append(inputs[i-60:i,0])
    X_test = np.array(X_test)
    
    X_test = np.reshape(X_test, (X_test.shape[0],X_test.shape[1],1))
    closing_price = model.predict(X_test)
    closing_price = scaler.inverse_transform(closing_price)
    #print(closing_price)
    
    rms=np.sqrt(np.mean(np.power((test-closing_price),2)))
    print('rms is:',rms)
    
    #for plotting
    train = df[:former]
    test = df[former:]
    test['Predictions'] = closing_price
    figure(figsize=(14, 8), dpi=80)
    plt.plot(train['close'])
    plt.plot(test[['close','Predictions']])
    
def main():
    global x_train
    global y_train
    global x_test
    global y_test
    global train
    global test
    crypto  = ['LTC']   # BTC LTC ETH
    grain   = '60'
    year    = '2021'
    #month   = '1,2,3,4,5,6,7,8,9,10,11,12'
    month   = '11'    
    day     = '1'
    try:
        for cur in crypto:        
            #get data into data frame
            #sql     = "select *  from cryptodb.vw_coinbase_"+cur+"_hist where grain_minute = "+grain+\
            #  " and YEAR  in ("+year+ ") and month  in ("+month+ ") order by timestamp_key asc"
            sql     = "select *  from cryptodb.vw_coinbase_"+cur+"_hist where grain_minute = "+grain+\
              " and YEAR  in ("+year+ ") and month  in ("+month+ ") and day in ("+day+ ") order by timestamp_key asc"
            dfAll = fetchData(sql)
            #print('Sample Dataframe is :',df.head())
            #performLinearRegression(dfAll)
            #performLogisticRegression(dfAll)
            #performSVM(dfAll)
            #performKNN(dfAll)
            performLSTM(dfAll)
    except Exception as e:
        print("Exception occurred",str(e))
    
if __name__ == '__main__':
    if main():
        True
    else:
        False
    