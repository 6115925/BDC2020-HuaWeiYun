import pandas as pd
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
port = pd.read_csv('data/lastport.csv')
port = port.drop_duplicates(['TRANS_NODE_NAME'],keep='first')
port = port.loc[:,['TRANS_NODE_NAME','LONGITUDE','LATITUDE']]
Oneport = pd.read_csv('data/Oneport.csv')

df = pd.read_csv('new/train大清洗.csv')
def dataShow(da):
    ck = da.drop_duplicates(['loadingOrder'], 'first')
    print(da.shape,ck.shape)

def isTrue(longitude,train_longitude,latitude,train_latitude):
    if abs(longitude - train_longitude) < 0.25 and abs(latitude - train_latitude) < 0.25:
        return True
    else :
        return False

'''去重'''
print('初始数据:')
dataShow(df)
df = df.drop_duplicates(['loadingOrder', 'carrierName', 'timestamp','vesselMMSI'],keep='first')
dataShow(df)

'''洗方向等于-1和大于36000'''
df = df[df['direction']>-1]
dataShow(df)
df = df[df['direction']<=36000]
print('方向在0~360范围内:')
dataShow(df)

'''洗速度过大'''
df = df[df['speed']<=55]
print('瞬时速度在55以内:')
dataShow(df)

'''洗一个订单多个路由'''
df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
print(df1.shape)
df1 = df1.groupby('loadingOrder')['loadingOrder'].agg(count='count').reset_index()
check = df1[df1['count']>1]
print('一个订单多个路由数量:',check.shape)
df1 = df1[df1['count']==1]
print('一个订单一个路由:',df1.shape)
df1.drop(['count'],axis=1,inplace=True)
df = df1.merge(df,on='loadingOrder',how='left')
print('一个订单一个路由:')
dataShow(df)


'''洗路由就一个港口'''
df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
print(df1.shape)
df1 = df1.loc[:,['loadingOrder','TRANSPORT_TRACE']]
data = df1.groupby('loadingOrder')['TRANSPORT_TRACE'].agg(count='count').reset_index()
df2 = data[data['count']==0]#无路由订单
print('无路由订单数量:',df2.shape)
data = data[data['count']==1]
df1 = data.merge(df1,on='loadingOrder',how='left')
print('有路由订单数量:',df1.shape)
df1['ok'] = df1['TRANSPORT_TRACE'].apply(lambda x:1 if x.count('-')!=0 else 0)
df1 = df1[df1['ok']==1]
print('去除一个港口的有路由订单数量:',df1.shape)
df1 = df1.loc[:,['loadingOrder']]
df2 = df2.loc[:,['loadingOrder']]
df1 = pd.concat([df1,df2],axis=0)
print(df1.shape)
df = df1.merge(df,on='loadingOrder',how='left')
print('一个路由两个及以上港口:')
dataShow(df)


'''一个订单多艘船'''
df1 = df.drop_duplicates(['loadingOrder','vesselMMSI'],'first')
print(df1.shape)
df1 = df1.groupby('loadingOrder')['loadingOrder'].agg(count='count').reset_index()
check = df1[df1['count']>1]
print('一个订单多艘船数量:\n',check)
k = df.drop_duplicates(['loadingOrder','vesselMMSI'],'first')
check = check.merge(k,on='loadingOrder',how='left')
check = check.loc[:,['loadingOrder','vesselMMSI','TRANSPORT_TRACE']]
check.sort_values(['TRANSPORT_TRACE','loadingOrder','vesselMMSI'],inplace=True)
# check.drop(['count'],axis=1,inplace=True)
# check = check.merge(df,on='loadingOrder',how='left')
check.to_csv('data/多艘船.csv',index=False)
df1 = df1[df1['count']==1]
print('一个订单一艘船:',df1.shape)
df1.drop(['count'],axis=1,inplace=True)
df = df1.merge(df,on='loadingOrder',how='left')
print('一个订单一艘船:')
dataShow(df)


'''洗同一条船同一个时间段'''
group = df.groupby(['loadingOrder'])['timestamp'].agg(max='max',min='min').reset_index()
print(group.shape)
df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
df1 = df1.loc[:,['loadingOrder','TRANSPORT_TRACE','vesselMMSI']]
print(df1.shape)
group = group.merge(df1,on='loadingOrder',how='left')
group = group.loc[:,['loadingOrder', 'min','max','vesselMMSI','TRANSPORT_TRACE']]
group.sort_values(['vesselMMSI','min','max'],inplace=True)
group = group.drop_duplicates(['vesselMMSI','min','max'],'first')
print(group.shape)
group = group.loc[:,['loadingOrder']]
df = group.merge(df,on='loadingOrder',how='left')
print('去除同一艘船同一个时间段:')
dataShow(df)
df.to_csv('new/train清洗.csv',index=False)

'''洗不到港(洗法1)'''
# df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
# print(df1.shape)
# group = df1.groupby(['loadingOrder'])['TRANSPORT_TRACE'].agg(count='count').reset_index()
# print(group,'\n',group.shape)
# df2 = group[group['count']==0]
# df2.drop(['count'],axis=1,inplace=True)
# print('无路由订单:',df2.shape)
# df1 = group[group['count']==1]
# print('有路由订单',df1.shape)
# df1 = df1.merge(df,on='loadingOrder',how='left')
# df1 = df1.drop_duplicates(['loadingOrder'],'last')
# df1['ok'] = df1['TRANSPORT_TRACE'].apply(lambda x:1 if x.count('-')!=0 else 0)
# df3 = df1[df1['ok']==0]
# df3 = df3.loc[:,['loadingOrder']]
# print('路由就一个港口订单:',df3.shape)
# df1 = df1[df1['ok']==1]
# df1['arrive'] = df1['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
# df1 = df1.merge(port,left_on='arrive',right_on='TRANS_NODE_NAME',how='left')
# def f(x):
#     if abs(x.longitude - x.LONGITUDE) < 0.25 and abs(x.latitude - x.LATITUDE) < 0.25 :
#         return 1
#     else:
#         return 0
# df1['k'] = df1.apply(f,axis=1)
# df1 = df1[df1['k']==1]
# df1 = df1.loc[:,['loadingOrder']]
# print('有路由且到港订单:',df1.shape)
# df1 = pd.concat([df1,df3,df2],axis=0)
# print('洗去不到港剩下订单:',df1.shape)
# df = df1.merge(df,on='loadingOrder',how='left')
# dataShow(df)
# print(df)
# df.to_csv('new/train清洗.csv',index=False)


'''洗不到港(洗法2)'''
# df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
# print(df1.shape)
# group = df1.groupby(['loadingOrder'])['TRANSPORT_TRACE'].agg(count='count').reset_index()
# print(group)
# df2 = group[group['count']==0]
# df2.drop(['count'],axis=1,inplace=True)
# print('无路由订单:',df2.shape)
# df1 = group[group['count']==1]
# print('有路由订单',df1.shape)
# df1 = df1.merge(df,on='loadingOrder',how='left')
# df1 = df1.drop_duplicates(['loadingOrder'],'last')
# df1['ok'] = df1['TRANSPORT_TRACE'].apply(lambda x:1 if x.count('-')!=0 else 0)
# df3 = df1[df1['ok']==0]
# df3 = df3.loc[:,['loadingOrder']]
# print('路由就一个港口订单:',df3.shape)
# df1 = df1[df1['ok']==1]
# df1['arrive'] = df1['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
# df1 = df1.merge(port,left_on='arrive',right_on='TRANS_NODE_NAME',how='left')
# def f(x):
#     if abs(x.longitude - x.LONGITUDE) < 0.25 and abs(x.latitude - x.LATITUDE) < 0.25 :
#         return 1
#     else:
#         return 0
# df1['k'] = df1.apply(f,axis=1)
# df4 = df1[df1['k']==1]
# df4 = df4.loc[:,['loadingOrder']]
# print('有路由且到港订单:',df4.shape)
# df1 = df1[df1['k']==0].reset_index(drop=True)
# print('没到港订单:',df1.shape)
# kk = 0
# result = pd.DataFrame()
# print(df1.columns)
# for j in range(df1.shape[0]):
#     id = df1.loc[j].loadingOrder
#     data = df[df['loadingOrder']==id].reset_index(drop=True)
#     flag1 = 0
#     # flag2 = 0
#     list_ = []
#     for i in range(data.shape[0]):
#         longitude = data.loc[i].longitude
#         latitude = data.loc[i].latitude
#         LONGITUDE = df1.loc[j].LONGITUDE
#         LATITUDE = df1.loc[j].LATITUDE
#         if flag1 ==1 :
#             list_.append(0)
#             continue
#         if isTrue(longitude,LONGITUDE,latitude,LATITUDE):
#             flag1 = 1
#         if flag1 ==1 :
#             list_.append(0)
#         else:
#             list_.append(1)
#     if flag1 == 1:
#         data['is'] = list_
#         data = data[data['is']==1]
#         data.drop(['is'],axis=1,inplace=True)
#         if kk ==0:
#             result = data
#             kk = 1
#         else:
#             result = pd.concat([result,data],axis=0)
#         print('id:',id,' ly:',df1.loc[j].TRANSPORT_TRACE)
# print('中间到港订单:')
# dataShow(result)
# df1 = pd.concat([df4,df3,df2])
# df = df1.merge(df,on='loadingOrder',how='left')
# print('无路由及到港订单:')
# dataShow(df)
# df = pd.concat([result,df],axis=0)
# print('无路由到港及中间到港:')
# dataShow(df)
# df.to_csv('new/train清洗2.csv',index=False)

'''
df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
print(df1.shape)
df1 = df1.groupby('loadingOrder')['TRANSPORT_TRACE'].agg(count='count').reset_index()
df = df.merge(df1,on='loadingOrder',how='left')
df = df[df['count']<2]
dataShow(df)

df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
print(df1.shape)
df1 = df1.loc[:,['loadingOrder','TRANSPORT_TRACE']]
df1 = df1.groupby('loadingOrder')['loadingOrder'].agg(count='count').reset_index()
print(df1)
df1 = df1[df1['count']==2]
print(df1)
# df1 = df1[df1.isnull().T.any()]
# print(df1)

df1 = df.drop_duplicates(['loadingOrder'],'first')
print(df1.shape)
df1 = df1.groupby('loadingOrder')['TRANSPORT_TRACE'].agg(count='count').reset_index()
df2 = df1[df1['count']==0]
print(df2.shape)
df1 = df1[df1['count']==1]
print(df1.shape)
df1['ok'] = df1['TRANSPORT_TRACE'].apply(lambda x:1 if x.count('-')!=0 else 0)
df1 = df1[df1['ok']==1]'''
