from __future__ import print_function
from pyspark import SparkConf, SparkContext

sc = SparkContext()

def low(l):
    l = l.upper()
    l = l.split()
    return l

myRDD = sc.textFile("file:/home/hduser/lower.txt")

out = myRDD.flatMap(low)

wc = out.map(lambda x: (x,1))
wc_grouped = wc.groupByKey()
wc_freq = wc_grouped.mapValues(sum).map(lambda x: (x[1],x[0])).sortByKey(False)

out.foreach(print)
wc.foreach(print)
wc_grouped.foreach(print)
wc_freq.foreach(print)
