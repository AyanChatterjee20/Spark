from pyspark import SparkConf, SparkContext
import re
from pyspark.sql import Row
from datetime import datetime


reg = '^(\S+)\s(\S+)\s(\S+)\s\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s-\d{4})]\s"(\S+)\s(\S+)(.*)"\s(\d{3})\s(\S+)$'

def logParsing(logline):
	if re.search(reg, logline):
		host = re.search(reg, logline).group(1)
	else:
		host = 'No Match'
	if re.search(reg, logline):
		timestamp = re.search(reg, logline).group(4)
	else:
		timestamp = 'No Match'
	if re.search(reg, logline):
		method = re.search(reg, logline).group(5)
		endpoint = re.search(reg, logline).group(6)
	else:
		method = 'No Match'
		endpoint = 'No Match'
	if re.search(reg, logline):
		status = re.search(reg, logline).group(8)
	else:
		status = '-99999'
	if re.search(reg, logline):
		content = re.search(reg, logline).group(9)
	else:
		content = '-1'
	line = host +';'+ timestamp +';'+ method +';'+ endpoint +';'+ status +';'+ content
	return line


conf = SparkConf().setAppName("Nasa Log Analytics of Aug-1995 & July-1995 in Spark-RDD using python").setMaster("yarn-client")
sc = SparkContext(conf=conf)


logs = sc.textFile('/InputFiles/Spark/access*')
lines = logs.map(logParsing)

#wrongLine = lines.filter(lambda x: (x.split(";")[0] == 'No Match' or x.split(";")[1] == 'No Match' or x.split(";")[2] == 'No Match' or x.split(";")[3] == 'No Match' or x.split(";")[4] == '-99999' or x.split(";")[5] == '-1'))
#total 67 record has data quality issue, so we are ignoring those 67 records

lineFiltered = lines.filter(lambda x: (x.split(";")[0] != 'No Match' or x.split(";")[1] != 'No Match' or x.split(";")[2] != 'No Match' or x.split(";")[3] != 'No Match' or x.split(";")[4] != '-99999' or x.split(";")[5] != '-1')).cache()

#top 10 requested URLs along with count of number of times they have been requested
#(This information will help company to find out most popular pages and how frequently they are accessed)
urlCount = lineFiltered.map(lambda l: (l.split(";")[3],1)).reduceByKey(lambda x, y: x + y)
urlCountOrdered = urlCount.coalesce(1).sortBy(lambda u: -u[1])


#top 5 hosts / IP making the request along with count
#(This information will help company to find out locations where website is popular or to figure out potential DDoS attacks)
hostCount = lineFiltered.map(lambda l: (l.split(";")[0],1)).reduceByKey(lambda x, y: x + y)
hostCountOrdered = hostCount.coalesce(1).sortBy(lambda u: -u[1])


#top 5 time frame for high traffic 
#(which day of the week or hour of the day receives peak traffic,
#this information will help company to manage resources for handling peak traffic load)
topHourCount = lineFiltered.map(lambda l: (l.split(";")[1].split(":")[1],1)).reduceByKey(lambda x, y: x + y)
topHourCountOrdered = topHourCount.coalesce(1).sortBy(lambda u: -u[1])

topDayCount = lineFiltered.map(lambda l: (int(l.split(";")[1].split("/")[0]),1)).reduceByKey(lambda x, y: x + y)
topDayCountOrdered = topDayCount.coalesce(1).sortBy(lambda u: -u[1])


#5 time frames of least traffic 
#(which day of the week or hour of the day receives least traffic, 
#this information will help company to do production deployment in that time frame so that less number of users will be affected if some thing goes wrong during deployment)
leastHourCount = lineFiltered.map(lambda l: (l.split(";")[1].split(":")[1],1)).reduceByKey(lambda x, y: x + y)
leastHourCountOrdered = leastHourCount.coalesce(1).sortBy(lambda u: u[1])

leastDayCount = lineFiltered.map(lambda l: (int(l.split(";")[1].split("/")[0]),1)).reduceByKey(lambda x, y: x + y)
leastDayCountOrdered = leastDayCount.coalesce(1).sortBy(lambda u: u[1])


#unique HTTP codes returned by the server along with count 
#(this information is helpful for devops team to find out how many requests are failing so that appropriate action can be taken to fix the issue)
statusCount = lineFiltered.map(lambda l: (l.split(";")[5],1)).reduceByKey(lambda x, y: x + y)



print("Top 10 requested URLs along with count of number of times they have been requested :")
for i in urlCountOrdered.take(10): print(i)
print(" ")

print("Top 5 hosts / IP making the request along with count :")
for i in hostCountOrdered.take(5): print(i)
print(" ")

print("Top 5 Hour in a day frame for high traffic :")
for i in topHourCountOrdered.take(5): print(i)
print(" ")

print("Top 5 day in a month frame for high traffic :")
for i in topDayCountOrdered.take(5): print(i)
print(" ")

print("Least 5 Hour in a day frame for low traffic :")
for i in leastHourCountOrdered.take(5): print(i)
print(" ")

print("Least 5 day in a month frame for low traffic :")
for i in leastDayCountOrdered.take(5): print(i)
print(" ")

print("Unique HTTP codes returned by the server along with count :")
for i in statusCount.take(10): print(i)

