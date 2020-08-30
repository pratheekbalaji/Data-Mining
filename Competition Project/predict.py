import xgboost
import pandas as pd
import json
from pyspark import SparkContext,SparkConf
import numpy as np
def avg(bus):
    result = {}

    for x in bus:
        if x[0] not in result:
            result[x[0]] = [x[1]]
        else:
            result[x[0]].append(x[1])
    for k in result:
        result[k] = sum(result[k]) / len(result[k])
    return result

def predict_ratings_item_based(x):
    user,business, similar_business =x[0] , x[1][0], x[1][1]
    # print(similar_business)
    sim = []

    for k in similar_business:

        if (business, k) in business_sim:
            sim.append((business_sim[(business, k)], similar_business[k]))
        if (k, business) in business_sim:
            sim.append((business_sim[(k, business)], similar_business[k]))

    if len(sim) == 0:
       if user in user_id_mean_dict:
           return user_id_mean_dict[user]
       else:
           return train_df['user_avg'].mean()
    sim = sorted(sim, key=lambda x: x[0], reverse=True)
    if len(sim) >= 5:
        sim = sim[:5]
    num = sum(x * y for x, y in sim)
    den = sum(i[0] for i in sim)
    pred = num / den

    return pred
appName = 'assignment7'
master = 'local[*]'
conf = SparkConf().setAppName(appName).setMaster(master)
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")


rdd1 = sc.textFile('train_review.json').map(json.loads).map(lambda x:((x['user_id']),x['business_id'],x['stars']))
train_features = rdd1.collect()
train_df = pd.DataFrame(train_features,columns=['user_id','business_id','stars'])
user_id_mean = pd.DataFrame(train_df.groupby('user_id').mean())
sr= train_df.groupby('user_id').mean()
user_id_mean_dict = sr.to_dict()
user_id_index= list(user_id_mean.index)
print(type(user_id_index))
user_id_count = pd.DataFrame(train_df.groupby('user_id')['business_id'].count())
user_id_mean.columns = ['user_avg']

user_id_count.columns = ['user_count']

business_id_mean = pd.DataFrame(train_df.groupby('business_id').mean())
business_id_count = pd.DataFrame(train_df.groupby('business_id')['stars'].count())

business_id_mean.columns = ['business_avg']
business_id_count.columns = ['business_count']
train_df = train_df.merge(user_id_mean,how='inner',on='user_id')
train_df = train_df.merge(user_id_count,how='inner',on='user_id')
train_df = train_df.merge(business_id_mean,how='inner',on='business_id')
train_df = train_df.merge(business_id_count,how='inner',on='business_id')
#train_df.drop(columns=['user_id','business_id'],axis=1,inplace=True)
train_x = train_df.loc[:,('user_avg','user_count','business_avg','business_count')]
train_y  = train_df.loc[:,('stars')]

test_df = pd.read_json('test_review.json',lines=True)
#print(test_df.count())
test_df =test_df.merge(user_id_mean,how='left',on='user_id')
test_df = test_df.merge(business_id_mean,how='left',on='business_id')
test_df = test_df.merge(business_id_count,how='left',on='business_id')
test_df =test_df.merge(user_id_count,how='left',on='user_id')

test_df["user_avg"] = test_df["user_avg"].fillna(value=test_df["user_avg"].mean())
test_df["user_count"] = test_df["user_count"].fillna(value=test_df["user_count"].mean())
test_df["business_avg"] = test_df["business_avg"].fillna(value=test_df["business_avg"].mean())
test_df["business_count"] = test_df["business_count"].fillna(value=test_df["business_count"].mean())
test_x = test_df.loc[:,('user_avg','user_count','business_avg','business_count')]
model = xgboost.XGBRegressor()
model.fit(train_x,train_y)
predictions = model.predict(test_x)
test_df = pd.concat((test_df.loc[:,('user_id','business_id')],pd.DataFrame(predictions,columns=['model_pred'])),axis=1)
test_df['model_pred'] = test_df['model_pred'].apply(lambda x: 5.0 if x>5.0 else x)
print(test_df.head())
rdd2 = sc.textFile('train.model').map(lambda x: json.loads(x))
rdd3  = sc.textFile('test_review.json').map(lambda x: json.loads(x)).map(lambda x: ((x['user_id'], x['business_id'])))
business_sim = rdd2.map(lambda x: ((x['b1'], x['b2']), (x['sim']))).collectAsMap()
rdd1 = sc.textFile('train_review.json').map(json.loads).map(lambda x: ((x['user_id'], (x['business_id'], x['stars']))))\
    .groupByKey().mapValues(list).mapValues(avg)
rdd3 = rdd3.leftOuterJoin(rdd1)
res = rdd3.map(lambda x: (((x[0], x[1][0]), predict_ratings_item_based(x)))).collect()
