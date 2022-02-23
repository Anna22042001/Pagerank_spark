import sys
from pyspark import SparkConf, SparkContext
import math


def to_tuple(line):
    line = line.split("\t")
    line = tuple(int(e) for e in line)
    return line


def to_matrix(lst, b):
    matrix = [[0 for i in range(1000)] for j in range(1000)]
    for e in lst:
        for f in e[1]:
            matrix[f - 1][e[0] - 1] = 1
    sum_list = [0 for j in range(1000)]
    for j in range(1000):
        for i in range(1000):
            sum_list[j] += matrix[i][j]
    for j in range(1000):
        for i in range(1000):
            matrix[i][j] /= sum_list[j]
            matrix[i][j] *= b
    return matrix


conf = SparkConf()
sc = SparkContext(conf=conf)
text = sc.textFile(sys.argv[1])
lst = text.map(to_tuple).distinct()
lst1 = lst.map(lambda c: (c[0], c[1])).groupByKey().distinct()
lst2 = lst.map(lambda c: (c[1], c[0])).groupByKey().sortByKey()
lst1 = lst1.mapValues(list)
lst2 = lst2.mapValues(list)
to_transform = lst1.collect()
# matrix with beta to transform (each column is source, row is destination)
value_matrix = to_matrix(to_transform, 0.9)
s = sum(value_matrix[:][0])
key = lst2.collect()
# to teleport
taxation = [1 / 1000 * 0.1 for i in range(1000)]
# initial vector
initial = [[1 / 1000 for i in range(1000)]]
x = sc.parallelize(initial)


# each step to transform
def next_step(values, keys, teleport, x):
    re = [0 for i in range(1000)]
    for e in keys:
        i, j_list = e
        for j in j_list:
            plus = values[i - 1][j - 1] * x[j - 1]
            re[i - 1] += plus
        re[i - 1] += teleport[i - 1]
    return re


for i in range(50):
    # print(x.collect())
    # x = x.map(lambda c: next_step(value_matrix, key, taxation, c))
    x = x.map(lambda c: next_step(value_matrix, key, taxation, c))
result = x.collect()[0]
final = list()
for i in range(len(result)):
    to_add = tuple((i + 1, result[i]))
    final.append(to_add)
final.sort(key=lambda b: b[1], reverse=True)
for e in final[:10]:
    print(f"{e[0]}\t{e[1]:5f}")
# # lst2 = lst2.map(lambda c: printr(c))
