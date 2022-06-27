import pandas as pd
import numpy as np
import datetime
import time
from scipy import stats

import warnings

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)

def Distance(lat1,lat2 ,lon1, lon2 ):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km * 1000

start = time.time()
train_data = pd.read_csv('data/复A_2345.csv')
test_data = pd.read_csv('data/test大清洗.csv')
end = time.time()


def maKedis(data):
    dis_list = []
    sum = 0
    k = 0
    lastdis = 0
    lent = data.shape[0]
    for i in range(lent):
        if (i == lent - 1) or (data.loc[i].loadingOrder != data.loc[i + 1].loadingOrder):
            k+=1
            print(('成功处理完第%s个,'%k),('总距离长度为：%s')%sum)
            end = time.time()
            # print('当前程序已运行：%s秒'%int(end-start))
            dis_all.append(lastdis)
            dis_now.append(sum)
            dis_list.append(sum)
            sum = 0
            lastdis = 0
            continue
        lat1 = data.loc[i].latitude
        lat2 = data.loc[i + 1].latitude
        lon1 = data.loc[i].longitude
        lon2 = data.loc[i + 1].longitude
        dis_all.append(lastdis)
        dis_now.append(sum)
        dis = Distance(lat1, lat2, lon1, lon2)
        lastdis = dis
        sum += dis
    data = data.drop_duplicates(['loadingOrder'],'first')
    data = data.loc[:, ['loadingOrder']]
    data['disALL'] = dis_list
    return data

check = test_data.drop_duplicates(['loadingOrder'],'first')
print('测试集的订单数量：',check.shape)
print('测试集初始数量：',test_data.shape)
test_data = test_data.drop_duplicates().reset_index(drop=True)
print('测试集去重数量：',test_data.shape)
#
dis_all = []
dis_now = []
data = maKedis(test_data)
test_data = test_data.merge(data,on='loadingOrder',how='left')
print(test_data.shape,len(dis_all),len(dis_now))
# test_data['diff_dis'] = dis_all
# test_data['dis_now'] = dis_now
test_data = test_data.drop_duplicates(['loadingOrder'],'first')
# test_data = test_data.loc[:,['loadingOrder','diff_dis','dis_now','disALL','TRANSPORT_TRACE']]
test_data = test_data.loc[:,['loadingOrder','disALL','TRANSPORT_TRACE']]
test_data.sort_values(['TRANSPORT_TRACE','disALL'],inplace=True)
test_data.to_csv('data/test大清洗_航线距离.csv',index=False)
#
# print('测试集航线距离保存完成')
# endd = time.time()
# print('当前程序已运行：%s秒'%int(endd-start))

check = train_data.drop_duplicates(['loadingOrder'],'first')
print('训练集的订单数量：',check.shape)

dis_all = []
dis_now = []
data = maKedis(train_data)
train_data = train_data.merge(data,on='loadingOrder',how='left')
print(train_data.shape,len(dis_all),len(dis_now))
# train_data['diff_dis'] = dis_all
# train_data['dis_now'] = dis_now
train_data = train_data.drop_duplicates(['loadingOrder'],'first').reset_index(drop=True)
# train_data = train_data.loc[:,['loadingOrder','diff_dis','dis_now','disALL','cate']]
# train_data.columns = ['loadingOrder','diff_dis','dis_now','disALL','TRANSPORT_TRACE']
train_data = train_data.loc[:,['loadingOrder','disALL','cate']]
train_data.columns = ['loadingOrder','disALL','TRANSPORT_TRACE']


train_data.sort_values(['TRANSPORT_TRACE','disALL'],inplace=True)
# train_data.drop(['ok'],axis=1,inplace=True)
train_data.to_csv('data/复A_2345_航线距离.csv',index=False)

print('存在测试集路由复A_2345航线距离保存完成')
endd = time.time()
print('当前程序已运行：%s秒'%int(endd-start))

print('测试集路线距离最大值：',test_data['disALL'].max(),'\n测试集路线距离最小值：',test_data['disALL'].min(),'\n测试集路线距离平均数：',test_data['disALL'].mean(),'\n测试集路线距离中位数：',test_data['disALL'].median(),'\n测试集路线距离众数：',stats.mode(test_data['disALL'])[0][0])
print('\n训练集路线复A_2345距离最大值：',train_data['disALL'].max(),'\n训练集路线复A_2345距离最小值：',train_data['disALL'].min(),'\n训练集路线复A_2345距离平均数：',train_data['disALL'].mean(),'\n训练集路线复A_2345距离中位数：',train_data['disALL'].median(),'\n训练集路线复A_2345距离众数：',stats.mode(train_data['disALL'])[0][0])
