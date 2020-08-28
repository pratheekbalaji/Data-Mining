from pyspark import SparkContext
from itertools import combinations, chain
from collections import defaultdict
import sys
import math
import time


def write_to_file(message, phase, file_name,mode):
    singles = []

    for val in phase:

        if type(val) is not tuple:

            singles.append(val)
        elif type(val) is tuple and len(val) == 1:
            for x in val:
                singles.append(x)

    temp = list(map(str, singles))
    temp = sorted(temp)

    singles = ','.join("('{}')".format(i) for i in temp)

    k = 2
    temp = [len(i) for i in phase if type(i) is tuple]

    n = max(temp)
    res = []
    while (k <= n):
        temp = []
        for val in phase:
            if type(val) is tuple and len(val) == k:
                temp.append(val)

        res.append(temp)
        k += 1
   
    with open(file_name, mode) as f:

        f.write(message)
        f.write('\n')
        f.write(singles)
        singles = singles.strip()

        f.write('\n\n')



        for val in res:

            data = [tuple(sorted(tuple(map(str, tup)))) for tup in val]

            data = sorted(data)
            val = ','.join("{}".format(i) for i in data)
            val = val.strip()

            f.write(val.strip())

            f.write('\n\n')


            
   


def overall_count(items, candidate_items):
    items = list(items)
    d = defaultdict(list)
    for candidate in candidate_items:
        if type(candidate) is not tuple:
            candidate = [candidate]
            temp = tuple(candidate)
        else:
            temp = candidate

        for item in items:
            if set(temp).issubset(item):
                if temp not in d:
                    d[temp] = 1

                else:
                    d[temp] += 1

    return d.items()


def get_frequent_items(items, candidate, sample_support):
    candidate_count = defaultdict()
    for val in candidate:
        val = tuple(sorted(val))
        for item in items:
            if set(val).issubset(item):
                if val not in candidate_count:
                    candidate_count[val] = 1
                else:
                    candidate_count[val] += 1
    frequent_items = [x for x in candidate_count if candidate_count[x] >= sample_support]
    return sorted(tuple(frequent_items))


def get_k_candidates(previous_candidates, k):
    combinations_k = []
    previous_frequents = list(previous_candidates)

    for i in range(len(previous_frequents) - 1):
        for j in range(i + 1, len(previous_frequents)):
           
            if previous_frequents[i][0:k- 2] == previous_frequents[j][0:k- 2]:
                combinations_k.append(tuple(set(previous_frequents[i]).union(set(previous_frequents[j]))))
            else:
                break
    return sorted(combinations_k)



def apriori(items):

    items = list(items)
    candidate_list = []
    singletons_count = defaultdict()
    sample_support = support * (len(items) / count_items)

    for item in items:
        for val in item:
            if val not in singletons_count:
                singletons_count[val] = 1
            else:
                singletons_count[val] += 1
    singleton_list = [x for x in singletons_count if singletons_count[x] >= sample_support]
    candidate_list.extend(tuple(singleton_list))
    # Generating Candidate Pairs
    candidate_pairs = list(combinations(candidate_list, 2))
    frequent_pairs = get_frequent_items(items, candidate_pairs, sample_support)
    k = 3

    candidate_list.extend(frequent_pairs)
    t = frequent_pairs

    while (True):
        candidate_combinations = get_k_candidates(t, k)

        t = get_frequent_items(items, candidate_combinations, sample_support)
        if len(t) == 0:
            break
        candidate_list.extend(t)
        k += 1

    return candidate_list


start = time.time()
sc = SparkContext()
sc.setLogLevel("ERROR")

rdd = sc.textFile(sys.argv[3])
header = rdd.first()
case = int(sys.argv[1])
if case == 1:
    required_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')) \
        .map(lambda x: ((x[0]), (x[1]))).groupByKey().mapValues(set)  # removes duplicate values in a basket
else:
    required_rdd = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')) \
        .map(lambda x: ((x[1]), (x[0]))).groupByKey().mapValues(set)
all_items = required_rdd.map(lambda x: x[1])

count_items = all_items.count()

support = int(sys.argv[2])


map_phase_1 = all_items.mapPartitions(apriori).map(lambda x: (x, 1))

reduce_phase_1 = map_phase_1.keys().distinct().collect()  # get_candidate items
map_phase_2 = all_items.mapPartitions(lambda x: overall_count(x, reduce_phase_1))
reduce_phase_2 = map_phase_2.reduceByKey(lambda x, v: x + v) \
    .filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

write_to_file('Candidates:', reduce_phase_1, sys.argv[4],'w')
write_to_file('Frequent Itemsets:', reduce_phase_2, sys.argv[4],'a')
end = time.time()

print('Duration:', end - start)



