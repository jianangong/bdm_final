from pyspark import SparkContext
from sys import argv
import csv
import pyproj
import shapely.geometry as geom
import rtree
import fiona.crs
import geopandas as gpd

def createIndex(shapefile):
    '''
    This function takes in a shapefile path, and return:
    (1) index: an R-Tree based on the geometry data in the file
    (2) zones: the original data of the shapefile
    
    Note that the ID used in the R-tree 'index' is the same as
    the order of the object in zones.
    '''
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def words_compare(str1,list2):
    list1 = str1.split(" ")
    output = False
    new_list = list1.copy()
    for i in range(2,min(8,len(list1))):
        for j in range(len(list1)-i+1):
            new_list.append(" ".join(list1[j:j+i]))
    for word in new_list:
        if word in list2:
            output = True
            return output,new_list
        else:
            continue
    return output,new_list

def matchZone(p, index, zones):
    match = None
    for idx in index.intersection((p.x, p.y, p.x, p.y)):
            # idx is in the list of shapes that might match
        if zones.geometry[idx].buffer(0).contains(p):
            match = (zones.loc[idx,"plctract10"], zones.loc[idx,"plctrpop10"])
    return match

def map_cities(p_id, records):
    import pyproj
    import shapely.geometry as geom
    import csv
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    
    f1 = open("drug_illegal.txt","r",encoding="UTF8")
    list1 = f1.read().split("\n")
    f1.close()
    f2 = open("drug_sched2.txt","r",encoding="UTF8")
    list2 = f2.read().split("\n")
    f2.close()
    drug_list = list1+list2
    
    
    index, zones = createIndex("500cities_tracts.geojson")

    reader = csv.reader(records, delimiter="|")
    counts = {}

    for i,row in enumerate(reader):
        match = None
        if "|" in row[0]:
            row = row[0].split("|")
        if len(row) < 7:
            continue
        if words_compare(row[6],drug_list):
            p = geom.Point(proj(float(row[2]), float(row[1])))
            if  p.is_valid:
                match = matchZone(p,index, zones)
            if match:
                counts[match] = counts.get(match, 0) + 1
    return list(counts.items())

def main(sc):
    import csv
    rdd = sc.textFile(argv[1])
    outputs = rdd.mapPartitionsWithIndex(map_cities).reduceByKey(lambda x,y: x+y). \
                        map(lambda x: (x[0][0],x[1]*1.0/(x[0][1]*1.0))).sortByKey(). \
                        collect()
    with open("results.csv","w") as f:
        writer = csv.writer(f)
        for row in outputs:
            writer.writerow(row)
    return outputs

if __name__=="__main__":
    sc = SparkContext()
    out = main(sc)
    print(out)
    
    
"""
spark-submit --conf spark.executorEnv.LD_LIBRARY_PATH=$LD_LIBRARY_PATH  --executor-cores 5  --num-executors 10 --executor-memory 20G  --files hdfs:///tmp/bdm/500cities_tracts.geojson,hdfs:///tmp/bdm/drug_illegal.txt,hdfs:///tmp/bdm/drug_sched2.txt   ex_credit_01.py hdfs:///tmp/bdm/tweets-100m.csv
"""