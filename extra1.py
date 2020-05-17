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

def createindex(shapefile1):
    import geopandas as gpd
    import rtree
    import fiona.crs
    tract= gpd.read_file(shapefile1).to_crs(fiona.crs.from_epsg(2263))

    
    indexr = rtree.Rtree()
 
    for idx, geometry in enumerate(tract.geometry):
        indexr.insert(idx,geometry.bounds)
   
    return (indexr,tract)


def findzone(p, indexr, tract):
    match_tract = indexr.intersection((p.x,p.y,p.x,p.y))

    for idx in match_tract:
        if tract.geometry[idx].buffer(0).contains(p):
            tract_name=tract.plctract10[idx]
            tract_pop=tract.plctrpop10[idx]
            return (tract_name,tract_pop)
    return None 

def processpop(pid,records):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263',preserve_units=True)
   
    indexr,tract=createindex('500cities_tracts.geojson')
    
#     if pid==0:
#         next(records)
 
    counts={}
    keyword=[]
    with open('drug_illegal.txt') as file:
        reader = csv.reader(file)
        for row in reader:
            keyword.append(row[0])
    with open('drug_sched2.txt') as file:
        reader = csv.reader(file)
        for row in reader:
            keyword.append(row[0])
    reader = csv.reader(records,delimiter='|')        
    for row in reader:
        if len(row) < 7:
            continue
        if  row[1]=='' or row[2]=='' or row[1]!=row[1] or row[2]!=row[2]:
             continue
        flag=0
        for i in keyword:
            if i in row[6]:
                flag=1
                break
        if(flag==1):
            p = geom.Point(proj(float(row[2]),float(row[1])))
            if p.is_valid:
                match = findzone(p,indexr,tract)
            if match:
                counts[match] = counts.get(match,0) + 1 
    return counts.items()
 

if __name__=='__main__':
    output=sys.argv[2]
    tweetdata=sys.argv[1]
    sc = SparkContext()
    tweet = sc.textFile(tweetdata).cache()
    counts = tweet.mapPartitionsWithIndex(processpop) \
            .reduceByKey(lambda x,y: x+y)\
             .map(lambda x:(x[0][0],x[1]/x[0][1]))\
             .sortByKey().saveAsTextFile(output)
