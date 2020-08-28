import os
import random
import math
import time
import sys
import json


class KMeans:
    def __init__(self, k, tolerance=0.01, max_iterations=100):
        self.k = k
        self.tolerance = tolerance
        self.max_iterations = max_iterations

    def fit(self, data_points,dic):

        self.centroids = {}

        for i in range(self.k):
            self.centroids[i] = dic[data_points[i]]
        for i in range(self.max_iterations):
            self.assignments = {}
            for j in range(self.k):
                self.assignments[j] = []
            for d in data_points:
                distances = []
                for c in self.centroids:
                    distances.append(math.sqrt(sum([(i - j) ** 2 for i, j in zip(self.centroids[c], dic[d])])))
                min_index = distances.index(min(distances))
                self.assignments[min_index].append(dic[d])

            prev_centroid = dict(self.centroids)
            for a in self.assignments:
                self.centroids[a] = [(sum(l) / len(l)) for l in zip(*self.assignments[a])]

            flag = True
            for c in self.centroids:

                original = prev_centroid[c]
                current = self.centroids[c]

                a = [(i - j) for i, j in zip(current, original)]
                o = original * 100

                res = [(i / j) for i, j in zip(a, o)]

                res = sum(res)

                if res > self.tolerance:
                    flag = False
            if (flag):

                break
        return self.assignments
def generate_intermediate_results(round_id,discard_set_stats,compressed_set_stats,retained_set):

    clusters_discard_set = len(discard_set_stats)
    n_discard_set = sum([v[0] for v in discard_set_stats])
    clusters_compressed_set = len(compressed_set_stats)
    n_compressed_set = sum([v[0] for v in compressed_set_stats])
    n_retained_set = len(retained_set)

    return (round_id, clusters_discard_set, n_discard_set, clusters_compressed_set, n_compressed_set, n_retained_set)

def update_stats(stats,index,pts):
    to_update = stats[index]
    n = to_update[0]
    sum_ = to_update[1]
    sum_sq = to_update[2]
    n+=1

    updated_sum = [(i+j) for i,j in zip(sum_,pts)]
    updated_sum_sq = [(j*j+i) for i,j in zip(sum_sq,pts)]
    stats[index] = (n,updated_sum,updated_sum_sq)


def merge_clusters(cluster_1stats,cluster_2stats,cluster_1points,cluster_2points,type):
    alpha = 4
    cluster_2points_copy = cluster_2points.copy()
    for idx_1, c in enumerate(cluster_2stats):

        flag = False
        n1 = c[0]
        c1_sum = c[1]

        for idx,d in enumerate(cluster_1stats):
            dis = []
            n2 = d[0]
            c2_sum = d[1]

            c2_sum_sq = d[2]
            for a, b in enumerate(c2_sum):
                point = c1_sum[a]/n1


                ci = b / n2
                # print(n)

                di = point - ci

                # print(sumsq[a])

                std = math.sqrt(c2_sum_sq[a] /n2 - (ci ** 2))

                if std == 0:
                    break
                # print(variance)
                t = math.sqrt((di / std) ** 2)

                dis.append(t)

            r = sum(dis)



            threshold = math.sqrt(len(c2_sum))

            if (r<alpha*threshold):

                s= cluster_2points[idx_1]
                del cluster_2points_copy[idx_1]
                cluster_1points[idx].extend(s)
                flag = True
                break
        if flag == False and type=='intermediate':

            k = len(cluster_1points)
            cluster_1points[k] = []
            s = cluster_2points[idx_1]

            cluster_1points[k].extend(s)
            del cluster_2points_copy[idx_1]

    if (type=='intermediate'):
        return  cluster_1points
    else:
        return cluster_1points,cluster_2points_copy





def generate_stats(points_set):
    res = []
    for k in points_set:
        sum_cluster = tuple([sum(x) for x in zip(*points_set[k])])

        n = len(points_set[k])
        sumsq = []
        for v in points_set[k]:
            sumsq.append(tuple([x ** 2 for x in v]))
        sumsq = tuple([sum(x) for x in zip(*sumsq)])
        res.append((n,sum_cluster,sumsq))
    return res

def mahalanobis_distance(x,points_set,dic,kind):
    alpha = 4
    candidates = []
    flag = False
    for idx,cluster in enumerate(points_set):

        n = cluster[0]

        dis = []

        sum_,sumsq = cluster[1],cluster[2]

        temp= dic[x]

        for a,b in enumerate(sum_):

            ci = b/n

            #print(n)


            di =temp[a] -ci

            #print(sumsq[a])

            std = math.sqrt(sumsq[a]/n -(ci**2))

            if std ==0:
                break
            #print(variance)
            t = math.sqrt((di/std)**2)

            dis.append(t)

        r = sum(dis)


        threshold = math.sqrt(len(sum_))

        if r < (alpha*threshold):
            candidates.append((idx,r))
    if len(candidates)!=0:
        candidates = sorted(candidates,key=lambda x:x[1])[0]
        a,b = candidates[0],candidates[1]

        if kind=='discard':
            discard_set[a].append(dic[x])
            flag = True
        if kind=='compressed':
            compressed_set[a].append(dic[x])
            flag = True
    return flag


start = time.time()
folder = 'hw5_data/test2'
files = []
dic = {}
intermediate_results = []

for r, d, f in os.walk(folder):
    for file in f:
        files.append(os.path.join(r, file))


with open(files[0]) as f:
    for line in f:
        line = line.split(',')
        line = [float(x) for x in line]
        dic[(line[0])] = tuple(line[1:])

values = list(dic.keys())

reverse_map = {}
for k, v in dic.items():
    reverse_map[v] = k
random.seed(20)
random_sample = random.sample(values, int(0.3 * len(dic)))
d = KMeans(10)
discard_set = d.fit(random_sample,dic)
print(len(dic))
discard_set_stats = generate_stats(discard_set)

print(discard_set_stats)

remaining = list(set(dic.keys()).difference(set(random_sample)))

c = KMeans(30)
compressed_set_temp = c.fit(remaining,dic)

print(compressed_set_temp[0])
#print(generate_stats(compressed_set_temp))
c = compressed_set_temp.copy()
retained_set = []
for k in compressed_set_temp:
    if len(compressed_set_temp[k])==1:
        s = compressed_set_temp[k][0]
        retained_set.append(reverse_map[s])
        del c[k]
    if len(compressed_set_temp[k])==0:
        del c[k]



print(c)
compressed_set = {}
t = 0
for k in c:
    compressed_set[t] = c[k]
    t+=1
compressed_set_stats = generate_stats(compressed_set)
print(compressed_set_stats)
print(len(retained_set))
round_id = 1
intermediate_results.append(generate_intermediate_results(round_id,discard_set_stats,compressed_set_stats,retained_set))
print(intermediate_results[0])
files = files[1:]

for i,a in enumerate(files):
    with open(a) as f:
        rand_dic = {}

        for line in f:
            line = line.split(',')
            line = [float(x) for x in line]
            rand_dic[(line[0])] = tuple(line[1:])
            dic[line[0]] = tuple(line[1:])

    for k, v in rand_dic.items():
        reverse_map[v] = k
    for pts in rand_dic:
        exists_compressed = None
        exists_discard = mahalanobis_distance(pts,discard_set_stats,dic,"discard")

        if exists_discard == False:
            exists_compressed = mahalanobis_distance(pts,compressed_set_stats,dic,"compressed")
        if exists_compressed == False:

            retained_set.append(pts)
    discard_set_stats = generate_stats(discard_set)
    compressed_set_stats = generate_stats(compressed_set)
    print(retained_set)


    if len(retained_set)>50:
        e = KMeans(30)
        cs_new = e.fit(retained_set,dic)
        retained_set = []

        temp = cs_new.copy()
        for k,v in cs_new.items():
            if (len(cs_new[k])==1):
                print('here')

                retained_set.append(reverse_map[v[0]])
                del temp[k]
            if (len(cs_new[k])==0):
                del temp[k]
        compressed_set_new = {}
        c = 0
        for k in temp:
            compressed_set_new[c] = temp[k]
            c+=1
    print(retained_set)

    if len(compressed_set_new)>0:
        cs_new_stats = generate_stats(compressed_set_new)

        compressed_set= merge_clusters(compressed_set_stats,cs_new_stats,compressed_set,compressed_set_new,'intermediate')

    compressed_set_stats = generate_stats(compressed_set)
    round_id+=1
    if i ==len(files)-1:
        break
    ir = generate_intermediate_results(round_id,discard_set_stats,compressed_set_stats,retained_set)
    intermediate_results.append(ir)
    print(intermediate_results)

discard_set,compressed_set= merge_clusters(discard_set_stats,compressed_set_stats,discard_set,compressed_set,'final')
discard_set_stats = generate_stats(discard_set)
compressed_set_stats = generate_stats(compressed_set)
intermediate_results.append(generate_intermediate_results(round_id,discard_set_stats,compressed_set_stats,retained_set))
print(intermediate_results[-1])
for k,v in discard_set.items():
    discard_set[k] = []
    for t in v:
        discard_set[k].append(str(reverse_map[t]))
if len(compressed_set)>0:
    for k, v in compressed_set.items():
        compressed_set[k] = []
        for t in v:
            compressed_set[k].append(str(reverse_map[t]))
if len(retained_set)>0:
    map(str,retained_set)
r = []
for k,v in discard_set.items():
    for t in v:
        r.append((t,k))
if len(compressed_set)>0:
    for k, v in compressed_set.items():
        for t in v:
            r.append((t,-1))


if len(retained_set)>0:
    for a in retained_set:
        r.append((a,-1))
r = dict(r)
with open('result.json', 'w') as fp:
    json.dump(r, fp)
with open('intermediate_results','w') as fp:
    s=('round_id','nof_cluster_discard','nof_point_discard','nof_cluster_compression','nof_point_compression','nof_point_retained')
    s = str(s)
    fp.write(s[1:-1])

    for r in intermediate_results:
        r = str(r)
        fp.write(r[1:-1])
        fp.write("\n")

