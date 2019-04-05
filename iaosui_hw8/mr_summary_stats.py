#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob
import numpy as np
import functools
import re

find_num = re.compile(r"[-0-9.]+")
class MySummaryStats(MRJob):
    
    def mapper(self, _, line):
        num = find_num.findall(line)
        if len(num)>1:
            label = num[0]
            numbers = float(num[1])
            yield (label, (numbers,1))
    
    def reducer(self, label, value):
        number = [i for i in value]
        total = sum([i[1] for i in number])
        mean = sum([i[0] for i in number])/total
        variance = sum([(i[0]-mean)**2 for i in number])/total
        
        yield label, (total, mean, variance)
        
if __name__=='__main__':
    MySummaryStats.run()
