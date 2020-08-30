from pyspark import SparkContext,SparkConf
import json
from itertools import  combinations
import math
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

def get_candidates(business):

    res = []
    for a, b in combinations(business, 2):
        u1 = business_rdd[a]
        u2 = business_rdd[b]
        r = u1.intersection(u2)
        if (len(r) >= 3):
            res.append((a, b))
    return set(res)


def pearson_similarity(pairs):
    a, b = pairs
    users_a = business_rdd[a]
    users_b = business_rdd[b]
    common_users = list(users_a.intersection(users_b))
    rating_1 = []
    rating_2 = []
    for user in common_users:
        r1 = business_users_stars_avg[user][a]
        r2 = business_users_stars_avg[user][b]
        rating_1.append(r1)
        rating_2.append(r2)
    avg_r1 = sum(rating_1) / len(rating_1)
    avg_r2 = sum(rating_2) / len(rating_2)
    numerator = 0
    denominator_1 = 0
    denominator_2 = 0
    for x, y in zip(rating_1, rating_2):
        n = (x-avg_r1 ) * (y-avg_r1)
        numerator += n
        d1 = (avg_r1 - x) ** 2
        d2 = (avg_r2 - y) ** 2
        denominator_1 += d1
        denominator_2 += d2
    denominator_1 = math.sqrt(denominator_1)
    denominator_2 = math.sqrt(denominator_2)
    denominator = denominator_1 * denominator_2
    if denominator == 0:
        return ((a, b), 0)
    cor = float(numerator / denominator)
    return (((a, b), cor))
appName = 'assignment7'
master = 'local[*]'
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

rdd = sc.textFile('train_review.json').map(lambda x: json.loads(x))

business_users_stars_avg = rdd.map(lambda x: ((x['user_id'], (x['business_id'], x['stars'])))) \
        .groupByKey().mapValues(list).mapValues(avg).collectAsMap()


    # find businesses who have atleast 3 corated
business_rdd = rdd.map(lambda x: (x['business_id'], x['user_id'])).groupByKey().mapValues(set).collectAsMap()
business_id = list(business_rdd.keys())
candidates = list(get_candidates(business_id))

candidates = sc.parallelize(candidates)

pearson_sim = candidates.map(pearson_similarity).filter(lambda x: x[1] > 0).collect()
with open('train.model', 'w') as f:
    for x in pearson_sim:
        d = {}
        d = {"b1": x[0][0], "b2": x[0][1], "sim": x[1]}
        json.dump(d, f)
        f.write('\n')