#!/usr/bin/env python
# coding: utf-8

from mrjob.job import MRJob
import re

find_word = re.compile(r"[a-zA-Z0-9]+")

class MYWordCount(MRJob):
    
    def mapper(self, _, line):
        for word in find_word.findall(line):
            yield (word.lower(), 1)
            
    def combiner(self, word, counts):
        yield (word, sum(counts))
    
    def reducer(self, word, counts):
        yield (word, sum(counts))
        
if __name__=='__main__':
    MYWordCount.run()