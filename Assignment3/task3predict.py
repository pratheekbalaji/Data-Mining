import time
from pyspark import SparkContext
import json, sys

def predict_rating_user_based(x):
    business,user,co_rated_users= x[0],x[1][0],x[1][1]
    r = user_avg[user]
    average_ratings_user = (r[0]/r[1])


    num = 0
    den = 0
    for u in co_rated_users:
        avg_u = user_avg[u]
        u_rating_b = co_rated_users[u]
        if avg_u[1]==0 or avg_u[1]==1:
            continue
        average_u = (avg_u[0]-u_rating_b)/(avg_u[1]-1)

        if (user,u) in user_sim:
            num+= average_u*user_sim[user,u]
            den+=user_sim[user,u]
        if (u,user) in user_sim:
            num += average_u *user_sim[u, user]
            den+= user_sim[u, user]
    if num==0 or den==0:
        return average_ratings_user
    pred = average_ratings_user + (num/den)
    if pred>5:
        return None
    return pred

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


def predict_ratings(x):
    user, business, similar_business = x[0], x[1][0], x[1][1]
    # print(similar_business)
    sim = []

    for k in similar_business:

        if (business, k) in business_sim:
            sim.append((business_sim[(business, k)], similar_business[k]))
        if (k, business) in business_sim:
            sim.append((business_sim[(k, business)], similar_business[k]))

    if len(sim) == 0:
        return None
    sim = sorted(sim, key=lambda x: x[0], reverse=True)
    if len(sim) >= 5:
        sim = sim[:5]
    num = sum(x * y for x, y in sim)
    den = sum(i[0] for i in sim)
    pred = num / den

    return pred


start = time.time()
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')

sc = SparkContext()
sc.setLogLevel('ERROR')

rdd = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x))
rdd3 = sc.textFile(sys.argv[2])
rdd2 = sc.textFile(sys.argv[3]).map(lambda x: json.loads(x))
res = None
if (sys.argv[5] == 'item_based'):
    rdd1 = rdd.map(lambda x: ((x['user_id'], (x['business_id'], x['stars'])))) \
        .groupByKey().mapValues(list).mapValues(avg)


    business_sim = rdd2.map(lambda x: ((x['b1'], x['b2']), (x['sim']))).collectAsMap()
    rdd3 = rdd3.map(lambda x: json.loads(x)).map(lambda x: ((x['user_id'], x['business_id'])))
    rdd3 = rdd3.join(rdd1)
    res = rdd3.map(lambda x: (((x[0], x[1][0]), predict_ratings(x)))).filter(lambda x: x[1] is not None).collect()



if (sys.argv[5]=='user_based'):

    user_sim = rdd2.map(lambda x: ((x['u1'], x['u2']), (x['sim']))).collectAsMap()
    rdd1 = rdd.map(lambda x: ((x['business_id'], (x['user_id'], x['stars'])))) \
        .groupByKey().mapValues(list).mapValues(avg)
    rdd3 = rdd3.map(lambda x: json.loads(x)).map(lambda x: ((x['business_id'], x['user_id'])))

    rdd3 =rdd3.join(rdd1)
    user_avg = rdd.map(lambda x:(((x['user_id']),x['stars']))).groupByKey().mapValues(list)\
               .map(lambda x:((x[0],(sum(x[1]),len(x[1]))))).collectAsMap()
    print(rdd3.take(1))
    res =rdd3.map(lambda x: (((x[1][0], x[0]), predict_rating_user_based(x)))).filter(lambda x: x[1] is not None).collect()

with open(sys.argv[4], 'w') as f:
    for x in res:
        d = {"user_id": x[0][0], "business_id": x[0][1], "stars": x[1]}
        json.dump(d, f)
        f.write('\n')

end = time.time()
print(end - start)


