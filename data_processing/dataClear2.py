import pandas as pd
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
def dataShow(da):
    ck = da.drop_duplicates(['loadingOrder'], 'first')
    print(da.shape,ck.shape)
'''df = pd.read_csv('data/train同船同时间段.csv')
dataShow(df)
group = df.groupby('loadingOrder')['vesselMMSI'].agg(cou='count').reset_index()
dataShow(group)
df = df.merge(group,on='loadingOrder',how='left')
print(df)
df.sort_values(['vesselMMSI','minn','maxx'],inplace=True)
print(df)
list_ = []
for i in range(df.shape[0]):
    if df.loc[i].cou !=1:
        list_.append(1)
        continue
    if df.loc[i].vesselMMSI == df.loc[i+1].vesselMMSI and df.loc[i].minn == df.loc[i+1].minn and df.loc[i].maxx < df.loc[i+1].maxx:
        list_.append(0)
    else:
        list_.append(1)
print(len(list_))
df['ok'] = list_
df = df[df['ok']==1]
df.drop(['ok'],axis=1,inplace=True)
print(df)
dataShow(df)
# df.to_csv('data/train同船同时.csv',index=False)
list_1 = []
df =df.reset_index(drop=True)
for i in range(df.shape[0]):
    if df.loc[i].cou !=1:
        list_1.append(1)
        continue
    if df.loc[i].vesselMMSI == df.loc[i-1].vesselMMSI and df.loc[i].maxx == df.loc[i-1].maxx and df.loc[i].minn > df.loc[i-1].minn:
        list_1.append(0)
    else:
        list_1.append(1)
print(len(list_1))
df['ok'] = list_1
df = df[df['ok']==1]
df.drop(['ok'],axis=1,inplace=True)
print(df)
dataShow(df)
df.to_csv('data/train同船同时间.csv',index=False)'''

'''df = pd.read_csv('new/train新清洗.csv')
dataShow(df)
group = df.groupby(['loadingOrder'])['timestamp'].agg(minn='min',maxx='max').reset_index()
print(group.shape)
df1 = df.drop_duplicates(['loadingOrder','TRANSPORT_TRACE'],'first')
df1 = df1.loc[:,['loadingOrder','TRANSPORT_TRACE','vesselMMSI']]
print(df1.shape)
data = group.merge(df1,on='loadingOrder',how='left')
data = data.loc[:,['loadingOrder', 'minn','maxx','vesselMMSI','TRANSPORT_TRACE']]
data.sort_values(['vesselMMSI','minn','maxx'],inplace=True)
data = data.loc[:,['loadingOrder','TRANSPORT_TRACE']]
print(data.shape)
group = df.groupby(['loadingOrder','vesselMMSI'])['timestamp'].agg(minn='min',maxx='max').reset_index()
group.sort_values(['loadingOrder','vesselMMSI','minn','maxx'],inplace=True)
print(group.shape)
data = data.merge(group,on='loadingOrder',how='left')
data = data.loc[:,['loadingOrder','minn','maxx','vesselMMSI','TRANSPORT_TRACE']]
group = data.groupby('loadingOrder')['vesselMMSI'].agg(cou='count').reset_index()
data = data.merge(group,on='loadingOrder',how='left')
print(data)
dataShow(data)
data.to_csv('data/多艘船同时清洗.csv',index=False)'''

df = pd.read_csv('data/同船时间段包括清洗.csv')
dataShow(df)
df.sort_values(['vesselMMSI','minn','maxx'],inplace=True)
df = df.reset_index(drop=True)
print(df)
list_ = []
for i in range(df.shape[0]):
    if df.loc[i].cou != 1:
        list_.append(1)
        continue
    flag = 0
    for j in range(i-1,-1,-1):
        if df.loc[j].vesselMMSI != df.loc[i].vesselMMSI:
            list_.append(1)
            flag = 1
            break
        if df.loc[j].vesselMMSI == df.loc[i].vesselMMSI and df.loc[j].minn <= df.loc[i].minn and df.loc[i].maxx <= df.loc[j].maxx:
            list_.append(0)
            flag = 1
            break
    if flag == 0:
        list_.append(1)
print(len(list_))
df['ok'] = list_
df = df[df['ok']==1]
df.drop(['ok'],axis=1,inplace=True)
print(df)
dataShow(df)
df.to_csv('data/train新清洗订单.csv',index=False)