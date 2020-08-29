from pyspark import SparkContext, SparkConf
from itertools import combinations
from collections import defaultdict
import time, sys, math

def calculate_modularity(nodes_removed, adjacency_list, m):
    community_label = 1
    community_members = {}
    for n in nodes_removed:
        if n in community_members:
            continue
        else:
            community_members[n] = community_label
            if n in adjacency_list:
                a, b = BFS(n, adjacency_list)
                a = a.keys()
                for val in a:
                    if val not in community_members:
                        community_members[val] = community_label
            else:
                continue
            community_label += 1
    disjoint_members = set(adjacency_list.keys()).difference(set(community_members.keys()))

    for n in disjoint_members:
        if n in community_members:
            continue
        else:
            community_members[n] = community_label
            if n in adjacency_list:
                a, b = BFS(n, adjacency_list)
                a = a.keys()
                for val in a:
                    if val not in community_members:
                        community_members[val] = community_label
            else:
                continue
            community_label += 1
    community_dict = {}

    for key, value in community_members.items():
        if value not in community_dict:
            community_dict[value] = [key]
        else:
            community_dict[value].append(key)
    modularity = 0
    for k in community_dict:
        users = community_dict[k]
        for j,k in list(combinations(users,2)):
            temp = 0
            if k in original_map[j] or j in original_map[k]:
                temp = 1
            modularity += temp -len(original_map[j])*len(original_map[k])/(2*m)
    modularity = modularity/(2*m)
    return community_dict, modularity


def get_candidate_users(l):
    res = []
    for a, b in list(combinations(l, 2)):
        c1 = user_business[a]
        c2 = user_business[b]
        common_elements = c1.intersection(c2)
        if len(common_elements) >= filter_threshold:
            res.append(((a, b)))
            res.append((b, a))
    return res


def BFS(start_node, adjacency_list):
    q = []
    visited = {}
    q.append(start_node)
    nodes_level_map = defaultdict(list)
    bfs = []
    level = 0

    visited[start_node] = True

    while (q):
        level += 1
        a = [i for i in q]
        r = [(i, level) for i in q]

        nodes_level_map[level] = a
        bfs.extend(r)

        for _ in range(len(q)):
            front = q.pop(0)
            neighbors = adjacency_list[front]
            for n in neighbors:
                if n not in visited:
                    visited[n] = True
                    q.append(n)

    bfs_nodes = dict(bfs)

    return bfs_nodes, nodes_level_map


def calculate_betweness(nodes_level, level_nodes_map):
    node_betweness = {}
    edge_betweness = {}
    levels = sorted(level_nodes_map.keys(), reverse=True)
    levels = levels[:-1]

    for l in levels:
        nodes = level_nodes_map[l]
        for c in nodes:
            if c not in node_betweness:
                node_betweness[c] = 1
            else:
                node_betweness[c] += 1
            neighbors = adjacency_list[c]
            # finding all parents for a given node
            all_parents = []
            for n in neighbors:
                if nodes_level[n] == l - 1:
                    all_parents.append(n)
            # checking for no of shortest paths
            if len(all_parents) == 1:
                for p in all_parents:
                    if p not in node_betweness:
                        node_betweness[p] = node_betweness[c]
                    else:

                        node_betweness[p] = node_betweness[c] + node_betweness[p]
                    e = tuple(sorted((c, p)))
                    if e not in edge_betweness:
                        edge_betweness[e] = node_betweness[c]
                    else:

                        edge_betweness[e] += node_betweness[c]
            else:
                spath = {}
                for p in all_parents:
                    ancestors = adjacency_list[p]
                    count = 0
                    for a in ancestors:
                        if nodes_level[a] == l - 2:
                            count += 1
                    spath[p] = count

                for p in all_parents:
                    if p not in node_betweness:
                        node_betweness[p] = (spath[p] / sum(spath.values())) * node_betweness[c]
                    else:
                        node_betweness[p] += (node_betweness[c] * (spath[p] / sum(spath.values())))

                    e = tuple(sorted((c, p)))
                    if e not in edge_betweness:
                        edge_betweness[e] = node_betweness[c] * (spath[p] / sum(spath.values()))
                    else:
                        edge_betweness[e] += (node_betweness[c] * (spath[p] / sum(spath.values())))

    return edge_betweness.items()


def compute_node(adjacency_list, node):
    a, b = BFS(adjacency_list, node)
    edge_betweness = calculate_betweness(a, b)

    return edge_betweness


start = time.time()
Conf = SparkConf() \
    .setAppName('hw4') \
    .setMaster('local[3]')

filter_threshold = int(sys.argv[1])
sc = SparkContext(conf=Conf)
sc.setLogLevel("ERROR")
rdd = sc.textFile(sys.argv[2])
header = rdd.first()
user_business = rdd.filter(lambda x: x != header).map(lambda x: x.split(',')).map(
    lambda x: ((x[0]), (x[1]))).groupByKey().mapValues(set).collectAsMap()
edges = get_candidate_users(user_business.keys())
m = len(edges)/2
adjacency_list = sc.parallelize(edges).groupByKey().mapValues(list).collectAsMap()
original_map = adjacency_list
vertices = sc.parallelize(adjacency_list.keys())
betweeness = vertices.flatMap(lambda x: compute_node(x, adjacency_list)).groupByKey().mapValues(list).mapValues(
    lambda x: sum(x) / 2).sortBy(lambda x: (-x[1], x[0][0])).collect()

with open(sys.argv[3], 'w') as f:
    for b in betweeness:
        f.write(str(b)[1:-1])

        f.write("\n")
nodes_removed = []
global_modularity = -9999
final_community = {}
while len(edges)!=0:
    
    
    max_betweeness = vertices.flatMap(lambda x: compute_node(x, adjacency_list)).groupByKey().mapValues(list).mapValues(lambda x: sum(x) / 2).takeOrdered(1, key = lambda x: -x[1])
    
    edges_to_remove = max_betweeness[0][0]
    
    reverse_order = (edges_to_remove[1], edges_to_remove[0])
    edges.remove(edges_to_remove)
    edges.remove(reverse_order)
    
    for n in edges_to_remove:
        if n not in nodes_removed:
            nodes_removed.append(n)
    adjacency_list = sc.parallelize(edges).groupByKey().mapValues(list).collectAsMap()
    vertices = sc.parallelize(adjacency_list.keys())
    community, modularity = calculate_modularity(nodes_removed, adjacency_list, m)
    if modularity > global_modularity:
        global_modularity = modularity
        final_community = community

community = final_community
print(global_modularity, len(final_community))
res = []
for k, v in community.items():
    res.append(sorted(v))
res = sorted(res,key=lambda x:(len(x),x))
with open(sys.argv[4], 'w') as f:
    for r in res:
        
        f.write(str(r)[1:-1])
        f.write('\n')

end = time.time()
print(end - start)






