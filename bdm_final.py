import fiona
import fiona.crs
import pandas as pd
import geopandas as gpd
from pyspark.sql.session import SparkSession
from pyspark import SparkContext
import sys

def processStreet(pid,records):
    import csv
    import re
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        
        if row[13]!='':    #get borocode
            row[13]=int(row[13])
        if row[28]!='':    #get full_street
            row[28]=row[28].lower()
        if row[10]!='':    #get st_label
            row[10]=row[10].lower()
            
        for i in [2, 3, 4, 5]:
            if row[i]!='':
                if row[i].isdigit():     #check whether only contain numbers
                    row[i] = float(row[i])


                else:
                    #change '-' into a float number
                    first, second = row[i].split('-')
                    second = str(int(second))
                    row[i] = float(first+'.'+second)   
            else:
                row[i] = 0.0
            
        
        yield (row[0],row[28],row[10],row[13],row[2],row[3],1)
        yield (row[0],row[28],row[10],row[13],row[4],row[5],0)


def processViolation(pid,records):
    import csv
    import re

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
        
        try:
            year =row[4].split('/')[2]
        except:
            continue
        if year not in ['2015','2016','2017','2018','2019']:
            continue
        street=row[24].lower()
        
        if row[21] in borocode.keys():
            boro = borocode[row[21]]
        else:
            continue
        
        number=row[23]
        if bool(re.search('[a-zA-Z]', number)):
            continue
        elif number.isdigit():
            houseno=float(number)
            is_left=houseno%2
        else:
            try:
                first, houseno = row[23].split('-')
                houseno = str(int(houseno))
                is_left=float(houseno)%2
                houseno=float(first+'.'+houseno)

            except:
                continue
        


        yield (year,street,boro,houseno,is_left)

        
def breaktoyear(records):
    for r in records:
        if r[0][1]=='2015':
            yield (r[0][0], (r[1], 0, 0, 0, 0))
        elif r[0][1]=='2016':
            yield (r[0][0], (0, r[1], 0, 0, 0))
        elif r[0][1]=='2017':
            yield (r[0][0], (0, 0, r[1], 0, 0))
        elif r[0][1]=='2018':
            yield (r[0][0], (0, 0, 0, r[1], 0))
        elif r[0][1]=='2019':
            yield (r[0][0], (0, 0, 0, 0, r[1]))
        else: 
            yield (r[0][0], (0, 0, 0, 0, 0))
          
        
def coef_ols(y, x=list(range(2015,2020))):
    import numpy as np
    x, y=np.array(x), np.array(y)
    xm=np.mean(x)
    ym=np.mean(y)
    numer=sum((x-xm)**2)
    denomi=sum((y-ym)*(x-xm))
    coef=denomi/numer

    return coef      
        
if __name__ == "__main__":
    output = sys.argv[1]
    sc = SparkContext()
    spark = SparkSession(sc)
    
    street1=sc.textFile('hdfs:///tmp/bdm/nyc_cscl.csv').mapPartitionsWithIndex(processStreet)
    violation = sc.textFile('hdfs:///tmp/bdm/nyc_parking_violation/').mapPartitionsWithIndex(processViolation)     

    stre = spark.createDataFrame(street1, ('physicalID', 'street','st_label' ,'boro', 'low', 'high', 'is_left'))
    viola = spark.createDataFrame(violation, ('year', 'street', 'boro', 'house_number', 'is_left'))
    
    filtering1 = [viola.boro == stre.boro, 
         viola.street == stre.street, 
         viola.is_left == stre.is_left, 
         (viola.house_number >= stre.low) & (viola.house_number <= stre.high)]
    filtering2 = [viola.boro == stre.boro, 
         viola.street == stre.st_label, 
         viola.is_left == stre.is_left, 
         (viola.house_number >= stre.low) & (viola.house_number <= stre.high)]

    
    vio_stre= stre.join(viola, filtering1, how='left').groupBy([stre.physicalID, viola.year]).count()
    vio_stre2= stre.join(viola, filtering2, how='left').groupBy([stre.physicalID, viola.year]).count()
    
    totalfinal=vio_stre.rdd.union(vio_stre2.rdd)
    totalfinal.map(lambda x: ((x[0], x[1]), x[2])) \
            .mapPartitions(breaktoyear) \
            .reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4])) \
            .sortByKey() \
            .mapValues(lambda x: x + (coef_ols(y=list(x)),)) \
            .map(lambda x: ((x[0],) + x[1]))\
            .saveAsTextFile(output)
