
import os

from pyspark.sql import SQLContext
from pyspark import SparkContext
from graphframes import  GraphFrame
os.environ["PYSPARK_SUBMIT_ARGS"] = ("--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11")

sc = SparkContext()

sqlContext = SQLContext(sc)
vertices = sqlContext.createDataFrame([
  ("a", "Alice", 34),
  ("b", "Bob", 36),
  ("c", "Charlie", 30),
  ("d", "David", 29),
  ("e", "Esther", 32),
  ("f", "Fanny", 36),
  ("g", "Gabby", 60)], ["id", "name", "age"])
edges = sqlContext.createDataFrame([
  ("a", "b", "friend"),
  ("b", "c", "follow"),
  ("c", "b", "follow"),
  ("f", "c", "follow"),
  ("e", "f", "follow"),
  ("e", "d", "friend"),
  ("d", "a", "friend"),
  ("a", "e", "friend")
], ["src", "dst", "relationship"])
g = GraphFrame(vertices, edges)
print(g)
def check_modularity(graph_as_dic_, starting_points_):

	partition_number = 1

	membership_dic = {}

	for start_point in starting_points_:

		if start_point in membership_dic:
			continue

		else:
			membership_dic[start_point] = partition_number
			visited = [start_point]
			while len(visited) > 0:
				node_ = visited.pop(0)
				try:
					connected_nodes = graph_as_dic_[node_]
				except KeyError:
					continue
				for c_node in connected_nodes:
					if c_node not in membership_dic:
						membership_dic[c_node] = partition_number
						visited.append(c_node)
			partition_number += 1

	disjoint_points = set(graph_as_dic_.keys()).difference(set(membership_dic.keys()))

	for start_point in disjoint_points:

		if start_point in membership_dic:
			continue
		else:
			membership_dic[start_point] = partition_number
			visited = [start_point]
			while len(visited) > 0:
				node_ = visited.pop(0)
				try:
					connected_nodes = graph_as_dic_[node_]
				except KeyError:
					continue
				for c_node in connected_nodes:
					if c_node not in membership_dic:
						membership_dic[c_node] = partition_number
						visited.append(c_node)
			partition_number += 1



	#print()

	community = {}

	for k, v in membership_dic.items():
		try:
			community[v].append(k)
		except KeyError:
			community[v] = [k]

	#print(community.keys())

	modularity = 0.0

	for community_num, members in community.items():
		for i in range(len(members)):
			for j in range(i+1, len(members)):
				a_ij = 0
				if members[j] in adjacency_matrix[members[i]]:
					a_ij = 1
				modularity += a_ij - len(adjacency_matrix[members[i]])*len(adjacency_matrix[members[j]])/(2*total_number_of_edges)
	return len(community), community, modularity/(2*total_number_of_edges)
