# uses python's spark library pyspark to read a large file consisting of random integers sperated by spaces and outputs the maximum integer given
# it appears more than 'k' number of times in the file. k = 50 in this example and Random2 is a file with 1000 random integers between 1 and 100.

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
