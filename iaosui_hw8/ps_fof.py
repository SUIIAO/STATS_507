#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkConf, SparkContext
import sys
import itertools

if len(sys.argv) != 3:
    print('Usage:' + sys.argv[0] + '<in> <out>')
    sys.exit(1)
inputlocation = sys.argv[1]
outputlocation = sys.argv[2]

conf = SparkConf().setAppName('Summation')
sc = SparkContext(conf=conf)

data = sc.textFile(inputlocation)
data = data.map(lambda line: [int(line.split()[0])] + [int(line.split()[i]) for i in range(1,len(line.split()))])
data = data.map(lambda x: [[x[0]]+list(i) for i in itertools.combinations([x[i] for i in range(1,len(x))],2)])
data = data.flatMap(lambda x: [(tuple(sorted(i)),1) for i in x])
data = data.groupByKey().mapValues(len).filter(lambda n: n[1]>1).map(lambda n: str(n[0][0])+' '+str(n[0][1])+' '+str(n[0][2]))

data.saveAsTextFile(outputlocation)
sc.stop()



















