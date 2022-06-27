import pandas as pd
import numpy as np
import warnings


warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
test_gps_path = 'data/R2 ATest 0711.csv'
test= pd.read_csv(test_gps_path)

test=test.drop_duplicates('loadingOrder',keep='first')

train_gps_path = 'new/train新版清洗.csv'
train= pd.read_csv(train_gps_path)


def distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

train['timestamp'] = pd.to_datetime(train['timestamp'], infer_datetime_format=True)
len=train.shape[0]
print(len)
k=1
for i in range(1,len):
        if (train.iloc[i].loadingOrder == train.iloc[i-1].loadingOrder):
            if train.iloc[i].speed==0:
                dis=distance(train.iloc[i].longitude,train.iloc[i].latitude,train.iloc[i-1].longitude,train.iloc[i-1].latitude)
                time = (train.iloc[i]['timestamp'] - train.iloc[i - 1]['timestamp']).total_seconds() / 3600
                spe = dis / time
                if spe>=3:
                    train.iloc[i].speed= round(spe)
        else :
            print('完成%s个订单'%k)
            k=k+1

train.to_csv('new/train速度.csv', encoding='utf-8', index=False)
