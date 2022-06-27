import pandas as pd
import numpy as np
import math
import datetime


starttime = datetime.datetime.now()
train_gps_path = 'data/train.csv'
train= pd.read_csv(train_gps_path)
print(train.shape[0])
endtime = datetime.datetime.now()
print('读完')
print('The time cost: ')
print(endtime - starttime)

train['timestamp'] = pd.to_datetime(train['timestamp'], infer_datetime_format=True)
def distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

list=[]
temp=train.drop_duplicates('loadingOrder','first').reset_index(drop=True)[3000:6000]
ring=temp[['loadingOrder']]
train=ring.merge(train,on='loadingOrder',how='left')
print(train)
for i in range(temp.shape[0]):
    k = 0
    king = train[train.loadingOrder == temp.iloc[i].loadingOrder].reset_index(drop=True)
    list.append(king.iloc[0])
    p =0
    for j in range(1,king.shape[0]):
        dis=distance(king.iloc[p].longitude,king.iloc[p].latitude,king.iloc[j].longitude,king.iloc[j].latitude)
        time=(king.iloc[j]['timestamp'] - king.iloc[p]['timestamp']).total_seconds() / 3600
        spe=dis/time
        if spe<=55:
            p=j
            list.append(king.iloc[j])

    print('第%d个订单'%i)
    endtime = datetime.datetime.now()
    print('读完')
    print('The time cost: ')
    print(endtime - starttime)
train=pd.DataFrame(list).reset_index(drop=True)
print(train.shape[0])
train.to_csv('new/train大清洗2.csv', encoding='utf-8', index=False)