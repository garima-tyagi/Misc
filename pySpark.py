# reads a large file consisting of random integers sperated by spaces and outputs the maximum integer given
# it appears for more than 'k' number of times in the file. k = 50 in this example.

import pyspark
import operator
# from collections import Counter

n = 50
list1=[]

with pyspark.SparkContext("local", "ABC") as sc:
    lines = sc.textFile("Random2.txt")
    rdd = lines.flatMap(lambda k: k.split(" ")).map(lambda count: (count, 1))

    counter = rdd.reduceByKey(operator.add)
    items = counter.sortBy(lambda x: x[1], False)
    for k, repetitions in items.toLocalIterator():
        print(k, ": ", repetitions)
        if int(repetitions) > n:
            list1.append(k)

    print("Maximum", max(list1))
