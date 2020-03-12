from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import *

#spark = SparkSession.builder.master('local').appName('csv Example').getOrCreate()

conf = SparkConf().setAppName("Nasa Log Analytics of Aug-2018 & July-2018 in Spark-DataFrame using python").setMaster("yarn-client")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

ordersDF = sqlContext.read.text('/InputFiles/retail_db/orders')
ordersDF.registerTempTable("temp")
splitted = sqlContext.sql("select split(value,',') as col from temp")
splitted.registerTempTable("orderDF")
orders = sqlContext.sql("select cast(col[0] as int) order_id, col[1] order_date, cast(col[2] as int) order_customer_id,col[3] order_status from orderDF")

orderItemsDF = sqlContext.read.text('/InputFiles/retail_db/order_items')
orderItemsDF.registerTempTable("temp")
splitted = sqlContext.sql("select split(value,',') as col from temp")
splitted.registerTempTable("orderDF")
orderItems = sqlContext.sql("select cast(col[0] as int) order_item_id, cast(col[1] as int) order_item_order_id, cast(col[2] as int) order_item_product_id, cast(col[3] as int) order_item_quantity, cast(col[4] as float) order_item_subtotal, cast(col[5] as float) order_item_product_price from orderDF")

#spark.conf.set('spark.sql.shuffle.partitions','2')

dailyProductRevenue = orders.where("order_status in ('COMPLETE','CLOSED')").join(orderItems, orders.order_id == orderItems.order_item_order_id).groupBy(orders.order_date, orderItems.order_item_product_id).agg(round(sum('order_item_subtotal'), 2).alias('product_revenue'))

dailyProductRevenueSorted = dailyProductRevenue.sort('order_date',dailyProductRevenue.product_revenue.desc())

dailyProductRevenueSorted.write.parquet('/OutputFiles/dailyProductRevenueSorted').coalesce(4)


