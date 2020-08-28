from pyspark import SparkContext
import json
from itertools import combinations
import time, math, sys
from collections import OrderedDict
import random

def pearson_similarity_user(pair):

    x,y = pair
    business_x = set(business_rdd_map[x])
    business_y = set(business_rdd_map[y])
    common_business = list(business_x.intersection(business_y))
    rating_1 = []
    rating_2 = []
    for b in common_business:
        r1 = business_users_stars_avg[x][b]
        r2 = business_users_stars_avg[y][b]
        rating_1.append(r1)
        rating_2.append(r2)
    avg_r1 = sum(rating_1) / len(rating_1)
    avg_r2 = sum(rating_2) / len(rating_2)
    numerator = 0
    denominator_1 = 0
    denominator_2 = 0
    for c, d in zip(rating_1, rating_2):
        n = (c-avg_r1 ) * (d-avg_r2 )
        numerator += n
        d1 = (c-avg_r1) ** 2
        d2 = (d-avg_r2 ) ** 2
        denominator_1 += d1
        denominator_2 += d2
    denominator_1 = math.sqrt(denominator_1)
    denominator_2 = math.sqrt(denominator_2)
    denominator = denominator_1 * denominator_2
    if denominator == 0:
        return ((x,y), 0)
    cor = float(numerator / denominator)
    return (((x, y), cor))



def min_hash(a,b,user):
    hash_values = [(a*x+b)%m for x in user]
    return min(hash_values)


def generate_signature(x):
    business,user = x
    signature =[min_hash(a,b,user) for a,b in zip(A,B)]
    return((business,signature))


def hash_signatures(business_signatures):

    business,signature = business_signatures
    # bucket size =32 rows = 1
    b = 32
    r = 1
    res = [((i, hash(tuple(sorted(signature[i * r:(i + 1) * r])))), [business]) for i in range(0, b)]
    return res


def jaccard_similarity(x):
    res = []
    l = list(combinations(x,2))

    for x,y in l:
        t = tuple(sorted((x, y)))
        x = set(business_rdd_map[x])
        y = set(business_rdd_map[y])

        a =x.union(y)
        b = x.intersection(y)

        sim = len(b)/len(a)

        if len(b)>=3 and sim>=0.01:
            res.append((t,sim))

    return res



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


sc = SparkContext()
start = time.time()


pearson_sim = None
rdd = sc.textFile(sys.argv[1]).map(lambda x: json.loads(x))


if (sys.argv[3] == 'item_based'):
    business_users_stars_avg = rdd.map(lambda x: ((x['user_id'], (x['business_id'], x['stars'])))) \
        .groupByKey().mapValues(list).mapValues(avg).collectAsMap()


    # find businesses who have atleast 3 corated
    business_rdd = rdd.map(lambda x: (x['business_id'], x['user_id'])).groupByKey().mapValues(set).collectAsMap()
    business_id = list(business_rdd.keys())
    candidates = list(get_candidates(business_id))

    candidates = sc.parallelize(candidates)
    pearson_sim = candidates.map(pearson_similarity).filter(lambda x: x[1] > 0.001).collect()
    with open(sys.argv[2], 'w') as f:
        for x in pearson_sim:
            d = OrderedDict()
            d = {"b1": x[0][0], "b2": x[0][1], "sim": x[1]}
            json.dump(d, f)
            f.write('\n')

if (sys.argv[3]=='user_based'):
    business_id = rdd.map(lambda x: x['business_id']).distinct().collect()
    m = len(business_id)
    business_dict = {}
    for a,b in enumerate(business_id):
        business_dict[b] = a
    signatures = 32

    A = random.sample(range(120, 200), signatures)
    B = random.sample(range(1, 120), signatures)

    business_rdd = rdd.map(lambda x: (x['user_id'], x['business_id']))\
        .map(lambda x: (x[0], business_dict[x[1]])).groupByKey().map(lambda x: (x[0], list(set(x[1]))))
    business_rdd_map = business_rdd.collectAsMap()
    business_users_stars_avg = rdd.map(lambda x: ((x['user_id'], (x['business_id'], x['stars'])))) \
        .map(lambda x:(x[0],(business_dict[x[1][0]],x[1][1])))\
             .groupByKey().mapValues(list).mapValues(avg).collectAsMap()

    signature = business_rdd.map(generate_signature)
    lsh_candidates = signature.flatMap(hash_signatures).reduceByKey(lambda x, v: x + v)\
        .filter(lambda x: len(x[1]) > 1).map(lambda x: x[1])



    lsh_pairs = lsh_candidates.map(jaccard_similarity).flatMap(lambda x:x).distinct().map(lambda x:(x[0]))  #(lambda x: list(sorted(combinations(x, 2)))).distinct()

    pearson_values =lsh_pairs.map(pearson_similarity_user).filter(lambda x:(x[1]>0)).collect()

    with open(sys.argv[2], 'w') as f:
        for x in pearson_values:
            d = OrderedDict()
            d = {"u1": x[0][0], "u2": x[0][1], "sim": x[1]}
            json.dump(d, f)
            f.write('\n')


end = time.time()
print(end - start)