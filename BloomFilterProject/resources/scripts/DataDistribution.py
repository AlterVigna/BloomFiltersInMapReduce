#!/usr/bin/env python
# coding: utf-8

# Script use to compute  frequency distribution of the rounded rating values considering a random sample of the dataset.


import random
import collections
import matplotlib.pyplot as plt

array = []
with open("data.tsv") as f:   
    for line in f:
        array.append(line)
        
samples=random.sample(array,100000)
mydict={}
rounded=[];
for sample in samples:
    splitted=sample.split("\t")
    roundRate=round(float(splitted[1]))
    rounded.append(roundRate)
    if (mydict.get(roundRate)==None):
        mydict[roundRate]=1
    else:
        mydict[roundRate]=mydict[roundRate]+1;  
mydictOrdered = collections.OrderedDict(sorted(mydict.items()))




import seaborn as sns
import pandas as pd
myGraphicStructure={"Rating_value":[],"Frequency":[]}
for x in range(1, 11):
    myGraphicStructure["Rating_value"].append(x)
    myGraphicStructure["Frequency"].append(mydictOrdered.get(x))
dataFrame=pd.DataFrame.from_dict(myGraphicStructure)   
dataFrame






# matplotlib histogram
plt.hist(dataFrame1['Freq'], color = 'blue', edgecolor = 'black')
plt.xlabel('Rating value', fontsize=18)
plt.ylabel('Frequency', fontsize=18)





