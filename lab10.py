import pyspark
from pyspark.sql import SQLContext
import pyproj
import datetime
import pyspark.sql.functions as sf
import pyspark.sql.window as sw

def filterBike(pId, lines):
    import csv
    for row in csv.reader(lines):
        if (row[6] == 'Greenwich Ave & 8 Ave' and row[3].startswith('2015-02-01')):
            yield (row[3][:19])

def filterTaxi(pId, lines):
    if pId==0:
        next(lines)
    import csv
    import pyproj
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    gLoc = proj(-74.00263761, 40.73901691)
    sqm = 1320**2
    for row in csv.reader(lines):
        try:
            dropOff = proj(float(row[5]), float(row[4]))
        except:
            continue
        sDistance = (dropOff[0]-gLoc[0])**2 + (dropOff[1]-gLoc[1])**2
        if sDistance<sqm: # 0.25 mile to feet 
            yield row[1][:19]


if __name__ == '__main__':
    sc = pyspark.SparkContext()
    sqlContext = SQLContext(sc)
    taxi = sc.textFile('hdfs:///data/share/bdm/yellow.csv.gz')
    bike = sc.textFile('hdfs:///data/share/bdm/citibike.csv')
    gBike = bike.mapPartitionsWithIndex(filterBike).cache()
    gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()
    gAll = gBike.map(lambda x: (x, 0)) + gBike.map(lambda x: (x, 1))   # concat two rdds
    df = sqlContext.createDataFrame(gAll, ('time', 'event'))
    df1 = df.select(df['time'].cast('timestamp').cast('long').alias('epoch'), 'event')
    df1.registerTempTable('gAll')
    window = sw.Window.orderBy('epoch').rangeBetween(-600, 0)
    df2 = df1.select('event', (1-sf.min(df1['event']).over(window)) \
                                                 .alias('has_taxi')) \
        .filter(df1['event'] ==1 ) \
        .select(sf.sum(sf.col('has_taxi')))

    df2.rdd.saveAsTextFile('output3')
