import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)

train = pd.read_csv('data/train新版清洗订单.csv')
train = train.groupby(['loadingOrder','vesselMMSI'])['timestamp'].agg(minn='min',maxx='max').reset_index()
train.sort_values(['vesselMMSI','minn','maxx','TRANSPORT_TRACE'],inplace=True)
print(train)
train.to_csv('data/新版同船同时间.csv',index=False)
