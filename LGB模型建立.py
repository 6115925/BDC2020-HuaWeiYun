import pandas as pd
from tqdm import tqdm#进度条
import numpy as np
import datetime
from sklearn.metrics import mean_squared_error, explained_variance_score
from sklearn.model_selection import KFold
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer


import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
# baseline只用到gps定位数据，即train_gps_path
train_gps_path = 'new/B_trainY3.csv'
test_data_path = 'data/Btest0711_ALL.csv'
# order_data_path = 'data/loadingOrderEvent.csv'
port_data_path = 'data/port.csv'
test_path = 'new/B_testY3.csv'

port_data = pd.read_csv(port_data_path)
test = pd.read_csv(test_path)

train= pd.read_csv(train_gps_path)

print(train.shape[0])

test_data = pd.read_csv(test_data_path)

print('1.数据读取完成！')
#选取经纬度、速度、方向做简单特征
def get_data(data, mode='train'):
    assert mode == 'train' or mode == 'test'
    if mode == 'train':
        1
    elif mode == 'test':
        data['temp_timestamp'] = data['timestamp']
        data['onboardDate'] = pd.to_datetime(data['onboardDate'], infer_datetime_format=True)
        data['loadingOrder'] = data['loadingOrder'].astype(str)
    data['timestamp'] = pd.to_datetime(data['timestamp'], infer_datetime_format=True)
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['speed'] = data['speed'].astype(float)
    # data['direction']=data['direction'].astype(float)
    data['TRANSPORT_TRACE'] = data['TRANSPORT_TRACE'].astype('category')
    data['vesselMMSI'] = data['vesselMMSI'].astype('category')
    data['carrierName'] = data['carrierName'].astype('category')
    return data


vesselMMSI = pd.concat([train['vesselMMSI'],test['vesselMMSI']],axis=0)

from sklearn.preprocessing import LabelEncoder
le = LabelEncoder().fit(vesselMMSI)
label = le.transform(vesselMMSI)
train['vesselMMSI']=le.transform(train['vesselMMSI'])
test['vesselMMSI']=le.transform(test['vesselMMSI'])
del vesselMMSI

carrierName=pd.concat([train['carrierName'],test['carrierName']],axis=0)
le = LabelEncoder().fit(carrierName)
train['carrierName']=le.transform(train['carrierName'])
test['carrierName']=le.transform(test['carrierName'])
del carrierName


TRANSPORT_TRACE=pd.concat([train['TRANSPORT_TRACE'],test['TRANSPORT_TRACE']],axis=0)
le = LabelEncoder().fit(TRANSPORT_TRACE)
train['TRANSPORT_TRACE']=le.transform(train['TRANSPORT_TRACE'])
test['TRANSPORT_TRACE']=le.transform(test['TRANSPORT_TRACE'])
del TRANSPORT_TRACE



train = get_data(train, mode='train')
test = get_data(test, mode='train')
test_data = get_data(test_data, mode='test')
print('2.类型转换完成！')





features = [c for c in train.columns if c not in ['loadingOrder','timestamp','label', 'min', 'max','carrierName','longitude_x','latitude_x','direction', 'count','go','arrive']]
print(features )
print('3.提取特征完成！')


feature_importances = pd.DataFrame() ##新建一个dataframe用来存放lgb模型判断的特征重要性
feature_importances['feature'] = features

#建模
def build_model(train, test, pred, label, seed=1080, is_shuffle=True):
    train_pred = np.zeros((train.shape[0],))
    test_pred = np.zeros((test.shape[0],))
    n_splits = 5
    # Kfold
    fold = KFold(n_splits=n_splits, shuffle=is_shuffle, random_state=seed)
    kf_way = fold.split(train[pred])
    # params
    params = {
        'learning_rate': 0.2,
        'boosting_type': 'gbdt',
        'objective': 'regression',
        'num_leaves': 63,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'seed': 8,
        'bagging_seed': 2,
        'feature_fraction_seed': 7,
        'min_data_in_leaf': 20,
        'nthread': -1,
        'verbose': 1,
        'device': 'cpu',
    }
    # train
    for n_fold, (train_idx, valid_idx) in enumerate(kf_way, start=1):
        train_x, train_y = train[pred].iloc[train_idx], train[label].iloc[train_idx]
        valid_x, valid_y = train[pred].iloc[valid_idx], train[label].iloc[valid_idx]
        # 数据加载
        n_train = lgb.Dataset(train_x, label=train_y)
        n_valid = lgb.Dataset(valid_x, label=valid_y)

        clf = lgb.train(
            params=params,
            train_set=n_train,
            num_boost_round=1200,
            valid_sets=[n_valid],
            early_stopping_rounds=12,
            verbose_eval=100,

        )
        # train_pred[valid_idx] = clf.predict(valid_x, num_iteration=clf.best_iteration)
        test_pred += clf.predict(test[pred], num_iteration=clf.best_iteration) / fold.n_splits
        feature_importances[f'fold_{n_fold + 1}'] = clf.feature_importance()
        print(feature_importances)
    test['label'] = test_pred
    return test[['loadingOrder', 'label']]

result = build_model(train, test, features, 'label', is_shuffle=True)
print('4.机器学习完成！')

test_data = test_data.merge(result, on='loadingOrder', how='left')
test['label']=result['label']*3600
test['ETA'] =(test['timestamp'] + test['label'].apply(lambda x: pd.Timedelta(seconds=x))).apply(
    lambda x: x.strftime('%Y/%m/%d  %H:%M:%S'))
test=test.loc[:,['loadingOrder','ETA']]

test_data=test_data.merge(test, on='loadingOrder', how='left')
test_data.drop(['direction', 'TRANSPORT_TRACE'], axis=1, inplace=True)
test_data['onboardDate'] = test_data['onboardDate'].apply(lambda x: x.strftime('%Y/%m/%d  %H:%M:%S'))
test_data['creatDate'] = pd.datetime.now().strftime('%Y/%m/%d  %H:%M:%S')
test_data['timestamp'] = test_data['temp_timestamp']
# 整理columns顺序
result = test_data[
    ['loadingOrder', 'timestamp', 'longitude', 'latitude', 'carrierName', 'vesselMMSI', 'onboardDate', 'ETA',
     'creatDate']]

result.to_csv('data/B_resultbase8.10.csv', index=False)
result = result.drop_duplicates(subset=['loadingOrder'],keep='first')
result.to_csv('data/B_base8.10.csv', index=False)