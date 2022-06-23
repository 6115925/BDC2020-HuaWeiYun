import pandas as pd
import numpy as np
import warnings
import datetime
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)

starttime = datetime.datetime.now()
train_gps_path = 'new/复B数据重做2.csv'
train= pd.read_csv(train_gps_path)
print(train.shape[0])

# train.drop_duplicates(['loadingOrder','carrierName','timestamp','vesselMMSI'],keep='first',inplace=True)
# print(train.shape[0])

def distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

port_data_path = 'data/port改.csv'
port_data = pd.read_csv(port_data_path)
port_data = port_data.drop_duplicates(['TRANS_NODE_NAME'],keep='first')
port_data =port_data[['TRANS_NODE_NAME','LONGITUDE','LATITUDE']]

# train=train[['loadingOrder','timestamp','longitude','latitude','carrierName','vesselMMSI','speed','TRANSPORT_TRACE' ]]
train['timestamp'] = pd.to_datetime(train['timestamp'], infer_datetime_format=True)
train.sort_values(['loadingOrder', 'timestamp'], inplace=True)
train=train[train['direction']!=-1]
train=train[train['direction']<=36000]
print(train.shape[0])
train=train[train['speed']<=55]
print(train.shape[0])

endtime = datetime.datetime.now()
print('The time cost: ')
print(endtime - starttime)

data= train.groupby('loadingOrder')['timestamp'].agg(max='max', count='count', min='min').reset_index()
data=data.merge(train, left_on=['loadingOrder', 'max'], right_on=['loadingOrder', 'timestamp'], how='left')


data=data[['loadingOrder','max' ]]
train=train.merge(data,on='loadingOrder',how='left')

train['arrive'] = train['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[-1])
train= train.merge(port_data, left_on='arrive', right_on='TRANS_NODE_NAME', how='left')



train=train[['loadingOrder', 'carrierName','timestamp', 'longitude','latitude', 'vesselMMSI', 'speed','direction','TRANSPORT_TRACE','LONGITUDE','LATITUDE','max']]

train.columns =['loadingOrder', 'carrierName','timestamp', 'longitude','latitude', 'vesselMMSI', 'speed','direction','TRANSPORT_TRACE','longitude_y','latitude_y','max']



train['disarrive']=distance(train.longitude,train.latitude,train.longitude_y,train.latitude_y)
train['label']= (train['max'] - train['timestamp']).dt.total_seconds() / 3600
train =train[[ 'carrierName','timestamp', 'longitude','latitude', 'vesselMMSI', 'speed','TRANSPORT_TRACE','longitude_y','latitude_y','disarrive','label']]
# train.drop_duplicates(['vesselMMSI','timestamp'],'first',True)
print(train.shape[0])
train.to_csv('new/B_trainY3.csv', encoding='utf-8', index=False)