The dataset contains two months’ worth of all HTTP requests to the NASA Kennedy Space Center WWW server in Florida of July-1995 and Aug-1995 which is having around 3.46 milion record and stored in HDFS :

[cloudera@quickstart python]$ hdfs dfs -ls /InputFiles/Spark
Found 2 items
-rw-r--r--   1 cloudera supergroup  167813770 2020-03-04 08:43 /InputFiles/Spark/access_log_Aug95
-rw-r--r--   1 cloudera supergroup  205242368 2020-03-04 08:44 /InputFiles/Spark/access_log_Jul95



Please find the random 10 records below : 

[cloudera@quickstart python]$ hdfs dfs -cat /InputFiles/Spark/access* | head
in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
uplherc.upl.com - - [01/Aug/1995:00:00:07 -0400] "GET / HTTP/1.0" 304 0
uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/ksclogo-medium.gif HTTP/1.0" 304 0
uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/MOSAIC-logosmall.gif HTTP/1.0" 304 0
uplherc.upl.com - - [01/Aug/1995:00:00:08 -0400] "GET /images/USA-logosmall.gif HTTP/1.0" 304 0
ix-esc-ca2-07.ix.netcom.com - - [01/Aug/1995:00:00:09 -0400] "GET /images/launch-logo.gif HTTP/1.0" 200 1713
uplherc.upl.com - - [01/Aug/1995:00:00:10 -0400] "GET /images/WORLD-logosmall.gif HTTP/1.0" 304 0
slppp6.intermind.net - - [01/Aug/1995:00:00:10 -0400] "GET /history/skylab/skylab.html HTTP/1.0" 200 1687
piweba4y.prodigy.com - - [01/Aug/1995:00:00:10 -0400] "GET /images/launchmedium.gif HTTP/1.0" 200 11853
slppp6.intermind.net - - [01/Aug/1995:00:00:11 -0400] "GET /history/skylab/skylab-small.gif HTTP/1.0" 200 9202



Each field	meaning :

remotehost -	Remote hostname (or IP number if DNS hostname is not available or if DNSLookup is off).
rfc931 -	The remote logname of the user if at all it is present.
authuser -	The username of the remote user after authentication by the HTTP server.
[date] -	Date and time of the request.
"request"	- The request, exactly as it came from the browser or client.
status -	The HTTP status code the server sent back to the client.
bytes -	The number of bytes (Content-Length) transferred to the client.
