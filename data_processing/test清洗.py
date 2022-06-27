import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
test_gps_path = 'data/Btest0711_ALL.csv'
test= pd.read_csv(test_gps_path)
def distance(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km


port_data_path = 'data/port.csv'
port_data = pd.read_csv(port_data_path)
port_data = port_data.drop_duplicates(['TRANS_NODE_NAME'],keep='first')
port_data =port_data[['TRANS_NODE_NAME','LONGITUDE','LATITUDE']]

print(test.shape)
test = test.drop_duplicates(keep='first')
print(test.shape)
print(test)
test['timestamp'] = pd.to_datetime(test['timestamp'], infer_datetime_format=True)

test=test.sort_values(['loadingOrder', 'timestamp']).reset_index(drop=True)


test['diff_days'] = test.groupby('loadingOrder')['timestamp'].diff(1).dt.total_seconds()/(3600*24)

print(test.shape[0])
list=[]
temp=test.drop_duplicates('loadingOrder','first').reset_index(drop=True)
ring=temp[['loadingOrder']]
test=ring.merge(test,on='loadingOrder',how='left')
for i in range(temp.shape[0]):
    k =0
    king = test[test.loadingOrder == temp.iloc[i].loadingOrder].reset_index(drop=True)
    list.append(king.iloc[0])
    p =0
    for j in range(1,king.shape[0]):
        dis=distance(king.iloc[p].longitude,king.iloc[p].latitude,king.iloc[j].longitude,king.iloc[j].latitude)
        time=(king.iloc[j]['timestamp'] - king.iloc[p]['timestamp']).total_seconds() / 3600
        spe=dis/time
        if spe<=55:
            p=j
            list.append(king.iloc[j])

test=pd.DataFrame(list).reset_index(drop=True)


list=[]
temp=test.drop_duplicates('loadingOrder','first').reset_index(drop=True)
for i in range(temp.shape[0]):
    k = 0
    king = test[test.loadingOrder == temp.loc[i].loadingOrder].reset_index(drop=True)
    for j in range(king.shape[0]):
        if (king.loc[j].diff_days > 5):
            k = 1
        if k == 0:
            list.append(king.loc[j])

test=pd.DataFrame(list).reset_index(drop=True)

test['temp_timestamp'] =test['timestamp']
test['temp_timestamp'] =pd.to_datetime(test['temp_timestamp'], infer_datetime_format=True)
# test['timestamp'] = pd.to_datetime(test['timestamp'], infer_datetime_format=True)
len=test.shape[0]
print(len)
k=1
for i in range(1,len):
        if (test.iloc[i].loadingOrder == test.iloc[i-1].loadingOrder):
            if test.iloc[i].speed==0:
                dis=distance(test.iloc[i].longitude,test.iloc[i].latitude,test.iloc[i-1].longitude,test.iloc[i-1].latitude)
                time = (test.iloc[i]['temp_timestamp'] - test.iloc[i - 1]['temp_timestamp']).total_seconds() / 3600
                spe = dis / time
                if spe>=3:
                    test.iloc[i].speed= round(spe)
        else :
            print('完成%s个订单'%k)
            k=k+1


test=test[test['direction']!=-1]
test=test[test['direction']<=36000]
print(test.shape[0])
test=test[test['speed']<=55]
print(test.shape[0])


test.drop(['diff_days','temp_timestamp'],axis=1,inplace=True)
print(test.shape[0])
print(test.columns)
print(test)
test.to_csv('data/Btest大清洗.csv',index=False)

'''
test['arrive'] = test['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[-1])
test= test.merge(port_data, left_on='arrive', right_on='TRANS_NODE_NAME', how='left')
print(test)
test=test[['loadingOrder','timestamp','longitude','latitude','speed','direction','carrierName', 'vesselMMSI','onboardDate','TRANSPORT_TRACE','LONGITUDE','LATITUDE']]
test.columns=['loadingOrder','timestamp','longitude','latitude','speed','direction','carrierName', 'vesselMMSI','onboardDate','TRANSPORT_TRACE','longitude_y','latitude_y']
test['disarrive']=distance(test.longitude,test.latitude,test.longitude_y,test.latitude_y)
test=test.drop_duplicates('loadingOrder','last').reset_index(drop=True)
print(test.shape[0])
test.to_csv('data/testX.csv', encoding='utf-8', index=False)
'''
