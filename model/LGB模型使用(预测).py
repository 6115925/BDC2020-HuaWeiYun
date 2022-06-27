import pandas as pd
import numpy as np
from sklearn.model_selection import KFold
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns',None)
test_data_path = 'data/Btest0711_ALL.csv'
test_path = 'new/B_testY444.csv'
test = pd.read_csv(test_path)
test_data = pd.read_csv(test_data_path)

def get_data(data, mode='test'):
    if mode == 'test_data':
        data['temp_timestamp'] = data['timestamp']
        data['onboardDate'] = pd.to_datetime(data['onboardDate'], infer_datetime_format=True)
        data['loadingOrder'] = data['loadingOrder'].astype(str)
    data['timestamp'] = pd.to_datetime(data['timestamp'], infer_datetime_format=True)
    data['longitude'] = data['longitude'].astype(float)
    data['latitude'] = data['latitude'].astype(float)
    data['speed'] = data['speed'].astype(float)
    data['TRANSPORT_TRACE'] = data['TRANSPORT_TRACE'].astype('category')
    data['vesselMMSI'] = data['vesselMMSI'].astype('category')
    return data

test = get_data(test, mode='test')
test_data = get_data(test_data, mode='test_data')








print('类型转换完成！')

features = ['longitude','latitude', 'vesselMMSI', 'speed','TRANSPORT_TRACE','longitude_y','latitude_y','disarrive']
print(features )
print('提取特征完成！')

test_pred = np.zeros((test.shape[0],))
n_splits = 5
for n_fold in range(1,6) :
    clf = lgb.Booster(model_file='model'+str(n_fold)+'.txt')
    test_pred += clf.predict(test[features], num_iteration=clf.best_iteration) / n_splits
test['label'] = test_pred
result =test[['loadingOrder', 'label']]
print('机器学习完成！')

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
result.to_csv('data/B_resultbase.csv', index=False)
result = result.drop_duplicates(subset=['loadingOrder'],keep='first')
result.to_csv('data/B_base.csv', index=False)