from pyspark import SparkContext
import sys
import pandas as pd
import geopandas as gpd
import fiona
import fiona.crs
from pyspark.sql.session import SparkSession
import warnings
warnings.filterwarnings("ignore")

sc = SparkContext()
spark = SparkSession(sc)


def processStreet(pid,records):
    import csv
    import re
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        if row[2]=='' or row[3]=='' or row[4]=='' or row[5]=='' or row[10]=='' or row[13]=='' or row[28]=='':
            continue
        row[13]=int(row[13])
                
       
        yield (row[0],row[28],row[10],row[13],row[2],row[3],row[4],row[5])
streets=sc.textFile('hdfs:///tmp/bdm/nyc_cscl.csv')
street=streets.mapPartitionsWithIndex(processStreet)
street=spark.createDataFrame(streets, ['physicalid','full_stree','st_label','borocode','l_low_hn','l_high_hn','r_low_hn','r_high_hn'])
street=street.toPandas()

hyphen=['l_low_hn','l_high_hn','r_low_hn','r_high_hn']
for i in hyphen:
    street[i] = street[i].apply(lambda x:tuple(map(int, x.split('-'))))
set2=set(street['physicalid'])


# def processstreet(pid,records):
#     import csv
#     import re
    
#     if pid==0:
#         next(records)
#     reader = csv.reader(records)
#     for row in reader:
#         if row[2]=='' or row[3]=='' or row[4]=='' or row[5]=='' or row[10]=='' or row[13]=='' or row[28]=='':
#             continue
#         row[13]=int(row[13])
                
       
#         yield (row[0],row[28],row[10],row[13],row[2],row[3],row[4],row[5])
        
def processViolation(pid,records):
    import csv
    import re
#     streets = gpd.read_file('/Users/jianangong/Downloads/NYC Street Centerline (CSCL).geojson').to_crs(fiona.crs.from_epsg(2263))
#     fields=['physicalid','full_stree','st_label','borocode','l_low_hn','l_high_hn','r_low_hn','r_high_hn']
#     street=streets[fields]
#     street['borocode'] = street['borocode'].astype(int)
#     street.dropna(inplace=True)
#     hyphen=['l_low_hn','l_high_hn','r_low_hn','r_high_hn']
#     for i in hyphen:
#         street[i] = street[i].apply(lambda x:tuple(map(int, x.split('-'))))
    borocode={'MAN':1,"MH":1,"MN":1,"NEWY:":1,"NEW Y":1,"NY":1,"BRONX":2,"BX":2,"PBX":2,"BK":3,"K":3,"KING":3,"KINGS":3,
         "Q":4,"QN":4,"QNS":4,"QU":4,"QUEEN":4,"R":5,"RICHMOND":5}
    if pid==0:
        next(records)
    reader = csv.reader(records)
    
    
    for row in reader:
        if len(row) < 42:
            continue
        if  row[4]=='' or row[23]=='' or row[24]=='' or row[21]=='':
             continue
        year=row[4].split('/')[2]
        if year not in ['2015','2016','2017','2018','2019']:
            continue
            
        number=row[23]
        if bool(re.search('[a-zA-Z]', number)):
            continue
        elif number.isdigit():
            integer=int(number)
            i_t=tuple(map(int, number.split('-')))
        elif '-' in number:
            i_t=tuple(map(int, number.split('-')))
            integer=i_t[1]
        
#         idnumber=findplace(row[24],row[21],i_t,integer)
        
        if integer%2!=0:
            id1=street.loc[((street["full_stree"].str.lower()==row[24].lower())\
                                         |(street["st_label"].str.lower()==row[24].lower()))&(street["borocode"]==borocode[row[21]])\
                                         &(street["l_low_hn"]<=i_t)&(street["l_high_hn"]>=i_t)]
#             yield ((year,id1.iat[0,0]),1)
        else:
            id1=street.loc[((street["full_stree"].str.lower()==row[24].lower())\
                                         |(street["st_label"].str.lower()==row[24].lower()))&(street["borocode"]==borocode[row[21]])\
                        &(street["r_low_hn"]<=i_t)&(street["r_high_hn"]>=i_t)]
        if len(id1)>0:
            yield ((year,id1.iat[0,0]),1)
    
         
def coef_ols(y, x=list(range(2015,2020))):
    import numpy as np
    x, y=np.array(x), np.array(y)
    xm=np.mean(x)
    ym=np.mean(y)
    numer=sum((x-xm)**2)
    denomi=sum((y-ym)*(x-xm))
    coef=denomi/numer

    return coef
            
def takeSecond(elem):
    return elem[0][1]








if __name__=='__main__':
            
    import os
    
    file_paths_raw = []
    for dirname, _, filenames in os.walk('hdfs:///tmp/bdm/nyc_parking_violations/'):
        for filename in filenames:
            file_paths_raw.append(os.path.join(dirname, filename))
    rdd=sc.parallelize([])
    for i in range(len(file_paths_raw)):
        locals()['violation'+str(i)] = sc.textFile(file_paths_raw[i]).cache()
        locals()['rdd'+str(i)]=locals()['violation'+str(i)].mapPartitionsWithIndex(processViolation).reduceByKey(lambda x,y:x+y)
        rdd=rdd.union(locals()['rdd'+str(i)])
    rddfinal=rdd.reduceByKey(lambda x,y:x+y)
    
#     streets = gpd.read_file('/Users/jianangong/Downloads/NYC Street Centerline (CSCL).geojson').to_crs(fiona.crs.from_epsg(2263))
#     fields=['physicalid','full_stree','st_label','borocode','l_low_hn','l_high_hn','r_low_hn','r_high_hn']
#     street=streets[fields]
#     street['borocode'] = street['borocode'].astype(int)
#     street.dropna(inplace=True)
#     hyphen=['l_low_hn','l_high_hn','r_low_hn','r_high_hn']
#     for i in hyphen:
#         street[i] = street[i].apply(lambda x:tuple(map(int, x.split('-'))))
        
    
    for year in ['2015','2016','2017','2018','2019']:
        locals()['v'+year] = rddfinal.filter(lambda x:x[0][0]==year).collect()
        set1=set([i[0][1] for i in locals()['v'+year]])
        zerocount=list(map(lambda x:((year,x),0),set2-set1))
        counttotal=locals()['v'+year]+zerocount
        counttotal.sort(key=takeSecond)
        locals()['count'+year]=sc.parallelize(counttotal)
        locals()['count'+year]=locals()['count'+year].map(lambda x:(x[0][1],x[1]))
    
    fivetotal=count2015.join(count2016).join(count2017).join(count2018).join(count2019).sortBy(lambda x: x[0])\
            .map(lambda x: (x[0],(x[1][0][0][0][0],x[1][0][0][0][1],x[1][0][0][1],x[1][0][1],x[1][1])))
            
    fivetotal.mapValues(lambda x:(x, coef_ols(list(x)))).map(lambda x: (x[0],x[1][0][0],x[1][0][1],x[1][0][2],x[1][0][3],x[1][0][4],x[1][1])).saveAsTextFile('finoutput')
            
            
            
            
