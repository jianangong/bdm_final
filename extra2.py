def createIndex(geojson):
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
    zones = gpd.read_file(geojson).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)
    
    
def getdict():
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263',preserve_units=True)
   

    wordcount={}
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

    return wordcount
    
def processwords(pid,records):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263',preserve_units=True)
   
    indexr,tract=createindex('500cities_tracts.geojson')
    
    worddict=getdict()
    keyword=[]
    counts={}
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
            print(row[6])
            for i in row[6].split(' '):
                fre.append((i,worddict[i]))
            fre1=set(fre)
            a=[i[0] for i in sorted(fre1, key=lambda x : x[1])[0:3]]
            print(a)

            for i in a:
                counts[i]=counts.get(i,0)+1
        else:
            continue
    return counts.items()
    
    
if __name__ == "__main__":
    output=sys.argv[2]
    tweetdata=sys.argv[1]
    sc = SparkContext()
    tweet = sc.textFile(tweetdata).cache()       
    freq = tweet.mapPartitionsWithIndex(processwords).top(100, key=lambda x: x[1])
    sc.parallelize(freq).saveAsTextFile(output)
