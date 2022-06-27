import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
Oneport = pd.read_csv('data/Oneport.csv')
port = pd.read_csv('data/port.csv')
port = port.drop_duplicates(['TRANS_NODE_NAME'],keep='first')
port =port[['TRANS_NODE_NAME','LONGITUDE','LATITUDE']]
def dataShow(da):
    ck = da.drop_duplicates(['loadingOrder'], 'first')
    print(da.shape,ck.shape)

def isTrue(longitude,train_longitude,latitude,train_latitude):
    if abs(longitude - train_longitude) < 0.25 and abs(latitude - train_latitude) < 0.25:
        return True
    else :
        return False

def distance(lon1, lon2, lat1, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

data = pd.read_csv('new/train速度.csv')
dataShow(data)
# train = data.drop_duplicates(['loadingOrder'],keep='first').reset_index(drop=True)
# train = train.loc[:,['loadingOrder']]

test = pd.read_csv('data/Btest0711_ALL.csv')
test['go'] = test['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[0])
test['arrive'] = test['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
test = test.merge(Oneport,left_on='go',right_on='port1',how='left')
test = test.merge(Oneport,left_on='arrive',right_on='port1',how='left')
test['TRANSPORT_TRACE'] = test['port2_x'] + '-' + test['port2_y']
test = test.merge(port,left_on='port2_x',right_on='TRANS_NODE_NAME',how='left')
test = test.merge(port,left_on='port2_y',right_on='TRANS_NODE_NAME',how='left')
test = test.loc[:,['TRANSPORT_TRACE','port2_x', 'port2_y','LONGITUDE_x', 'LATITUDE_x','LONGITUDE_y', 'LATITUDE_y']]
test.columns = ['TRANSPORT_TRACE','go', 'arrive','go_lon', 'go_lat','arr_lon', 'arr_lat']
test = test.drop_duplicates(['TRANSPORT_TRACE'],keep='first').reset_index(drop=True)[60:66]
test = test.reset_index(drop=True)
print(test)
flag = 0
ind = 100000
k = 0
result = pd.DataFrame()
for j in range(test.shape[0]):
    go_lon = test.loc[j].go_lon
    go_lat = test.loc[j].go_lat
    arr_lon = test.loc[j].arr_lon
    arr_lat = test.loc[j].arr_lat
    trace = test.loc[j].TRANSPORT_TRACE
    data['diff_lon_go'] = abs(data['longitude']-go_lon)
    data['diff_lat_go'] = abs(data['latitude']-go_lat)
    data['diff_lon_arr'] = abs(data['longitude'] - arr_lon)
    data['diff_lat_arr']  = abs(data['latitude'] - arr_lat)
    train = data[data['diff_lon_go'] <= 0.6]
    train = train[train['diff_lat_go'] <= 0.6]
    train = train.drop_duplicates(['loadingOrder'], keep='first').reset_index(drop=True)
    train = train.loc[:, ['loadingOrder']]
    print('满足到出发港:', trace, train.shape)
    train = train.merge(data, on='loadingOrder', how='left')
    train = train[train['diff_lon_arr'] <= 0.6]
    train = train[train['diff_lat_arr'] <= 0.6]
    train = train.drop_duplicates(['loadingOrder'],keep='first').reset_index(drop=True)
    train = train.loc[:, ['loadingOrder']]
    print('满足到目的港:',trace,train.shape)
    list_ = []
    # train.to_csv('trace/%s.csv' % (trace), index=False)
    for b in range(train.shape[0]):
        flag1 = 0
        flag2 = 0
        l = -1
        r = -1
        zz = train.loc[b].loadingOrder
        df = data[data['loadingOrder']==zz].reset_index(drop=True)
        df['dis'] = distance(df['longitude'], arr_lon, df['latitude'], arr_lat)
        # df['ok'] = 0
        lent = df.shape[0]
        for i in range(lent):
            # print('YES')
            if flag1 == 0 and df.loc[i].speed != 0 and df.loc[i].diff_lon_go <=0.25 and df.loc[i].diff_lat_go <=0.25:#还没找到起点,现在找到了
                flag1 = 1
                # df.loc[i].ok = 1
                # df['dis'] = distance(df['longitude'],arr_lon,df['latitude'],arr_lat)
                # df['diff_lon'] = abs(df['longitude'] - arr_lon)
                # df['diff_lat'] = abs(df['latitude'] - arr_lat)
                l = i
                continue
            # if flag2 == 1 and (df.loc[i-1].speed == 0):
            #     continue
            if flag1 == 1 and (df.loc[i].dis <= 30 or (df.loc[i].diff_lon_arr <=0.25 and df.loc[i].diff_lat_arr <=0.25)) and df.loc[i].speed == 0:
                # if i > lent -3 or df.loc[i].vesselMMSI != df.loc[i+1].vesselMMSI:
                #     print(df.loc[i].dis,arr_lon,df.loc[i].longitude,arr_lat,df.loc[i].latitude)
                #     flag2 = 1
                #     r = i
                #     break
                # elif df.loc[i+1].speed == 0 and df.loc[i+2].speed == 0:
                print(df.loc[i].dis,arr_lon,df.loc[i].longitude,arr_lat,df.loc[i].latitude)
                flag2 = 1
                r = i
                break
            if i == lent - 1 and flag1 == 1 and (df.loc[i].diff_lon_arr <= 0.25 and df.loc[i].diff_lat_arr <= 0.25):
                print(df.loc[i].dis, df.loc[i].speed, arr_lon, df.loc[i].longitude, arr_lat, df.loc[i].latitude)
                flag2 = 1
                r = i
                break
            if i == lent -1 :
                break
            if df.loc[i].vesselMMSI != df.loc[i + 1].vesselMMSI and flag1 == 1 and (df.loc[i].diff_lon_arr <= 0.25 and df.loc[i].diff_lat_arr <= 0.25):
                print(df.loc[i].dis, df.loc[i].speed, arr_lon, df.loc[i].longitude, arr_lat, df.loc[i].latitude)
                flag2 = 1
                r = i
                break
        if flag2 == 1:
            list_.append(1)
            data1 = df[l:r+1]
            ind += 1
            data1['loadingOrder'] = str(ind)
            data1['TRANSPORT_TRACE'] = trace
            data1.drop(['diff_lon_go','diff_lat_go','diff_lon_arr','diff_lat_arr'],axis=1,inplace=True)
            if flag == 0:
                result = data1
                flag = 1
            else:
                result = pd.concat([result,data1])
            k += 1
            print('当前到第%s种路由'%j,'路由为:%s'%trace,',训练集到第%s'%b,'个,处理完第%s份订单'%k,data1.shape)
        else:
            list_.append(0)
    train['ok'] = list_
    train = train[train['ok']==1]
    train.to_csv('trace/%s.csv' % (trace), index=False)

print('共有%s份订单'%k)
dataShow(result)
result.to_csv('new/B数据重做11.csv',index=False)
# if flag1 == 1 and (df.loc[i].dis <= 40 or isTrue(arr_lon,df.loc[i].longitude,arr_lat,df.loc[i].latitude)==True) and df.loc[i].speed == 0:#已经找到起点，找终点
#     if flag2 == 1 and diss - df.loc[i].dis > 1:
#         diss = df.loc[i].dis
#         # df.loc[i].ok = 1
#         r = i
#     else:
#         flag2 = 1
#         diss = df.loc[i].dis
#         # df.loc[i].ok = 1
#         r = i
#     continue
