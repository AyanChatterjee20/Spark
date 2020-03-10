from pyspark import SparkConf, SparkContext
import re
from pyspark.sql import Row, SQLContext
from datetime import datetime
from pyspark.sql.functions import *


conf = SparkConf().setAppName("Nasa Log Analytics of Aug-1995 & July-1995 in Spark-DataFrame").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


reg = '^(\S+)\s(\S+)\s(\S+)\s\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s-\d{4})]\s"(\S+)\s(\S+)(.*)"\s(\d{3})\s(\S+)$'

logs = sqlContext.read.text("/InputFiles/Spark/access*")
#logs.filter(logs.value.isNull()).count()

def load_df(logs):
    logsDf = logs.select(regexp_extract('value', reg, 1).alias('Host'),
                         regexp_extract('value', reg, 4).alias('Timestamp'),
                         regexp_extract('value', reg, 5).alias('Method'),
                         regexp_extract('value', reg, 6).alias('Endpoint'),
                         regexp_extract('value', reg, 8).cast('integer').alias('Status'),
                         regexp_extract('value', reg, 9).cast('integer').alias('Content_size'))
    return logsDf

def filter_func(col_name,logsDf):
    return sum(logsDf[col_name].isNull().cast('integer')).alias(col_name)
    

logsDf = load_df(logs)
#logsDf.show(10,truncate=False)

#checking the null values column wise
countNull = [filter_func(col,logsDf) for col in logsDf.columns]
tempDf = logsDf.coalesce(1).agg(*countNull)
#tempDf.show()


#33905 null values in Content_size and it is : '-' and replacing it with 0
#logs.filter(~logs['value'].rlike(r'\s(\d+)$')).show()
logsDf = logsDf.na.fill({'Content_size' : 0})

countNull = [filter_func(col,logsDf) for col in logsDf.columns]
tempDf = logsDf.coalesce(1).agg(*countNull)
#tempDf.show()

#dropping the null values of Status :
logsDf = logsDf[logsDf.Status.isNotNull()]

#ignoring the timezone in timestamp and dropping the Timestamp and adding the new cloumn Time
logsDf = logsDf.select('*', split(logsDf.Timestamp, " ")[0].alias('Time')).drop('Timestamp')
logsDf.cache()


#content_size Statistics :
contentSizeSummary = logsDf.coalesce(1).describe(['Content_size'])
print("Content statistics of count , mean ,standard deviation , min, max : ")
contentSizeSummary.show()

#HTTP Status wise count :
HTTPcount = logsDf.coalesce(1).groupBy('Status').count().sort('Status')
print("Differnt HTTP Status wise count in ascending order :")
HTTPcount.show()

#Analyzing top 10 frequent hosts :
top10Hosts = logsDf.coalesce(1).groupBy('Host').count().sort('count', ascending = False).limit(10)
print("Top 10 frequest hosts :")
top10Hosts.show()

#top 10 error endpoint ,i.e., Status is other than '200' :
errorEndpoints = logsDf.coalesce(1).filter(logsDf.Status != 200).groupBy('Endpoint').count().sort('count', ascending = False).limit(10)
print("Top 10 error endpoints whose status is other than 200 :")
errorEndpoints.show(truncate = False)

#unique hosts daily in entire log (includes the day of the month and the associated number of unique hosts for that day) :
hostDay = logsDf.coalesce(1).select(logsDf.Host , split(logsDf.Time, ":")[0].alias('Day'))
uniqueHostDay = hostDay.dropDuplicates()
hostPerDay = uniqueHostDay.coalesce(1).groupBy('Day').count()
print("Count of unique number of hosts per day :")
hostPerDay.show(truncate = False)

#Hourly basis 404 errors count:
notFound = logsDf.coalesce(1).filter(logsDf.Status == 404)
hourly404Count = notFound.coalesce(1).groupBy(split(split(notFound.Time, ":")[1], ":")[0].alias('Hour')).count().sort('Hour')
print("Hourly basis 404 error count :")
hourly404Count.show(truncate = False)

from pyspark.sql.functions import col

#Average number of request daily on per host per day:
hostDay = logsDf.coalesce(1).select(logsDf.Host , split(logsDf.Time, ":")[0].alias('Day'))
uniqueHostDay = hostDay.coalesce(1).dropDuplicates()
hostPerDay = uniqueHostDay.coalesce(1).groupBy('Day').count().select(col('Day'), col('count').alias('Total_Hosts'))
reqPerDay = hostDay.coalesce(1).groupBy('Day').count().select(col('Day'), col('count').alias('Total_Requests'))
avgReqPerDay = hostPerDay.coalesce(1).join(reqPerDay, 'Day')
avgReqPerDay = avgReqPerDay.coalesce(1).withColumn ('Avg_Req', (col('Total_Requests')/col('Total_Hosts')))
print("Average number of request daily on per host per day:")
avgReqPerDay.show(truncate = False)


