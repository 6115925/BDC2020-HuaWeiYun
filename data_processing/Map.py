import pandas as pd
import geopandas
import matplotlib.pyplot as plt
from geopandas import GeoDataFrame, read_file
from geopandas.tools import sjoin
# from geopandas import polygons
from shapely.geometry import LineString,Point, mapping,shape
from shapely.geometry import Polygon
import warnings
import random
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows',None)
'''红色代表测试集，蓝色训练集'''
Oneport = pd.read_csv('data/Oneport.csv')
print(Oneport.shape)
Oneport = Oneport.drop_duplicates(['port1'],keep='first')
print(Oneport.shape)
# trace = pd.read_csv('data/复A_2345修改路由.csv')
port = pd.read_csv('data/port.csv')
port = port.drop_duplicates(['TRANS_NODE_NAME'],keep='first')
port = port.loc[:,['TRANS_NODE_NAME','LONGITUDE','LATITUDE']]
# trace = trace.loc[:,['TRANSPORT_TRACE']]

# df1=pd.read_csv('data/test清洗.csv')
df2=pd.read_csv('data/数据10.csv')
print(df2.shape)
df2 = df2.drop_duplicates(keep='last')
# df2['TRANSPORT_TRACE'] =  'CNYTN-CAVAN'
# check = df2.drop_duplicates(['loadingOrder'],keep='first')
# print(check.shape)
df2['cate'] = df2['TRANSPORT_TRACE']
# df2['TRANSPORT_TRACE'] = df2['cate']
# df2['TRANSPORT_TRACE'] = df2['TRANSPORT_TRACE'].astype(str)
# df2 = df2.drop_duplicates(['loadingOrder'],keep='first')
# print(df2)

df2['go'] = df2['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[0])
df2['arrive'] = df2['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
df2 = df2.merge(Oneport,left_on='go',right_on='port1',how='left')
df2 = df2.merge(Oneport,left_on='arrive',right_on='port1',how='left')
df2['TRANSPORT_TRACE'] = df2['port2_x'] + '-' + df2['port2_y']
# print(df2.shape)

df2.sort_values(['TRANSPORT_TRACE','loadingOrder'],inplace=True)
ID = df2.drop_duplicates(['loadingOrder'],'first').reset_index(drop=True)
# ID = ID.drop_duplicates(['loadingOrder'],'first').reset_index(drop=True)[5000:5300],'cate'
ID = ID.loc[:,['loadingOrder','TRANSPORT_TRACE','cate']]
print(ID.shape)


'''xy = [Point(xy) for xy in zip(df1.longitude,df1.latitude)]
pointDataFrame1 = geopandas.GeoDataFrame(df1,geometry=xy)


xy2 = [Point(xy) for xy in zip(df2.longitude,df2.latitude)]
pointDataFrame2 = geopandas.GeoDataFrame(df2,geometry=xy2)

fig, ax = plt.subplots()
world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))

world.plot(figsize = (100,120),ax=ax, cmap='YlGn')

# pointDataFrame1.plot(figsize = (200,220),ax=ax, marker='o', color='b', markersize=0.1)
pointDataFrame2.plot(figsize = (200,220),ax=ax, marker='x', color='r', markersize=0.1)
plt.show()
'''

trace = df2.loc[:,['TRANSPORT_TRACE']]
trace = trace.drop_duplicates(['TRANSPORT_TRACE'],'first').reset_index(drop=True)
# trace['TRANSPORT_TRACE'] = trace['TRANSPORT_TRACE'].astype(str)
# trace['go'] = trace['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[0])
# trace['arrive'] = trace['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
# trace = trace.merge(Oneport,left_on='go',right_on='port1',how='left')
# trace = trace.merge(Oneport,left_on='arrive',right_on='port1',how='left')
# trace['TRANSPORT_TRACE'] = trace['port2_x'] + '-' + trace['port2_y']
# trace = trace.loc[:,['TRANSPORT_TRACE']]
trace['go'] = trace['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[0])
trace['arrive'] = trace['TRANSPORT_TRACE'].apply(lambda x: x.split('-')[x.count('-')])
trace = trace.merge(port,left_on='go',right_on='TRANS_NODE_NAME',how='left')
trace = trace.merge(port,left_on='arrive',right_on='TRANS_NODE_NAME',how='left')
trace = trace.loc[:,['TRANSPORT_TRACE','LONGITUDE_x','LATITUDE_x','LONGITUDE_y', 'LATITUDE_y']]
print(trace)
# trace.to_csv('data/Atest首尾统一路由.csv',index=False)
# for i in range(trace.shape[0]):
#     w = trace.loc[i].TRANSPORT_TRACE
#     print(w)
#     df = df2[df2['TRANSPORT_TRACE'] == w]
#     tmp = trace[trace['TRANSPORT_TRACE'] == w]

for i in range(ID.shape[0]):
    id = ID.loc[i].loadingOrder
    w = ID.loc[i].TRANSPORT_TRACE
    z = ID.loc[i].cate
    df = df2[df2['loadingOrder']==id]
    tmp = trace[trace['TRANSPORT_TRACE'] == w]

    xy_go = [Point(xy) for xy in zip(tmp.LONGITUDE_x, tmp.LATITUDE_x)]
    pointDataFrame_go = geopandas.GeoDataFrame(tmp,geometry=xy_go)
    xy_arr = [Point(xy) for xy in zip(tmp.LONGITUDE_y, tmp.LATITUDE_y)]
    pointDataFrame_arr = geopandas.GeoDataFrame(tmp,geometry=xy_arr)

    xy = [Point(xy) for xy in zip(df.longitude, df.latitude)]
    pointDataFrame = geopandas.GeoDataFrame(df, geometry=xy)

    fig, ax = plt.subplots()
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    world.plot(figsize=(100, 120), ax=ax, cmap='Oranges')
    pointDataFrame_go.plot(figsize=(200, 220), ax=ax, marker='x', color='k', markersize=80)
    pointDataFrame_arr.plot(figsize=(200, 220), ax=ax, marker='x', color='c', markersize=80)
    
    pointDataFrame.plot(figsize=(200, 220), ax=ax, marker='x', color='r', markersize=3)
    s = w + '-' + str(id)
    tit = str(id) + '-' + z
    plt.title(tit,fontsize='x-small',fontweight='light')
    plt.savefig('地图/检查路线/%s.jpg'%s)
    plt.show()
