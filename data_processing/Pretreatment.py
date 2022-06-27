import pandas as pd
import numpy as np
import datetime


starttime = datetime.datetime.now()
train_gps_path = 'data/train0711.csv'
train= pd.read_csv(train_gps_path)
endtime = datetime.datetime.now()
print('读完')
print('The time cost: ')
print(endtime - starttime)
train.columns = ['loadingOrder', 'carrierName', 'timestamp', 'longitude',
                      'latitude', 'vesselMMSI', 'speed', 'direction', 'vesselNextport',
                      'vesselNextportETA', 'vesselStatus', 'vesselDatasource', 'TRANSPORT_TRACE']
train.drop(['vesselNextport','vesselStatus','vesselDatasource','vesselNextportETA'],axis=1,inplace=True)
train['timestamp'] = pd.to_datetime(train['timestamp'], infer_datetime_format=True)
train['longitude'] = train['longitude'].astype(float)
train['loadingOrder'] = train['loadingOrder'].astype(str)
train['latitude'] = train['latitude'].astype(float)
train['speed'] = train['speed'].astype(float)
train['direction'] = train['direction'].astype(float)
train['vesselMMSI'] = train['vesselMMSI'].astype(str)
train['carrierName'] = train['carrierName'].astype(str)
train['TRANSPORT_TRACE'] = train['TRANSPORT_TRACE'].astype(str)
train=train.sort_values(['loadingOrder','timestamp']).reset_index(drop=True)

print(train.shape[0])
endtime = datetime.datetime.now()
print('The time cost: ')
print(endtime - starttime)
train.drop_duplicates(keep='first',inplace=True)
print(train.shape[0])
endtime = datetime.datetime.now()
print('The time cost: ')
print(endtime - starttime)

train.to_csv('data/train.csv', index=False)
endtime = datetime.datetime.now()
print('The time cost: ')
print(endtime - starttime)




