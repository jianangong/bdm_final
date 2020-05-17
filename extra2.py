import fiona
import fiona.crs
import shapely
import rtree
import pyproj
import shapely.geometry as geom
import sys
import pandas as pd
import geopandas as gpd
from pyspark import SparkContext


    
    
wordcount={}
keyword=[]
with open('drug_illegal.txt') as file:
    reader = csv.reader(file)
    for row in reader:
        keyword.append(row[0])
with open('drug_sched2.txt') as file:
    reader = csv.reader(file)
    for row in reader:
        keyword.append(row[0])
with open(sys.argv[1]) as file:
    reader = csv.reader(file,delimiter='|')      
    for row in reader:
        if len(row) < 7:
            continue
        if len(row[6])<3:
            continue
        flag=0
        for i in keyword:
            if i in row[6]:
                flag=1
                break
        if flag==1:
            row[6]=row[6].replace(',','')
            for i in row[6].split(' '):
                wordcount[i]=wordcount.get(i,0)+1

    
def processwords(pid,records):
    import csv
    
   
    counts={}
    reader = csv.reader(records,delimiter='|')    
    for row in reader:
        if len(row) < 7:
            continue
        if len(row[6].split(' '))<3:
            continue
        flag = 0
        fre=[]
        for i in keyword:
            if i in row[6]:
                flag=1
                break
        if(flag==1):
            row[6]=row[6].replace(',','')
            for i in row[6].split(' '):
                fre.append((i,wordcount[i]))
            fre1=set(fre)
            a=[i[0] for i in sorted(fre1, key=lambda x : x[1])[0:3]]
            
            for i in a:
                counts[i]=counts.get(i,0)+1
        else:
            continue
    return counts.items()
    
if __name__ == "__main__":
    output=sys.argv[2]
    tweetdata=sys.argv[1]
    sc = SparkContext()
    tweet = sc.textFile('/Users/jianangong/Downloads/tweet.csv').cache()       
    freq = tweet.mapPartitionsWithIndex(processwords).top(100, key=lambda x: x[1])
    sc.parallelize(freq).saveAsTextFile(output)
