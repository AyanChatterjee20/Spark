from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Daily Revenue Per Product").setMaster("yarn-client")
sc = SparkContext(conf=conf)

orders = sc.textFile("/InputFiles/retail_db/orders")
orderItems = sc.textFile("/InputFiles/retail_db/order_items")

orderFilter = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"])

ordersMap = orderFilter.map(lambda o: (int(o.split(",")[0]),o.split(",")[1].split(" ")[0]))
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]),(int(oi.split(",")[2]),float(oi.split(",")[4]))))

orderJoin = ordersMap.join(orderItemsMap)

orderJoinMap = orderJoin.map(lambda o: ((o[1][0],o[1][1][0]),round(o[1][1][1],2)))
dailyRevenuePerProduct = orderJoinMap.reduceByKey(lambda x,y: x + y)

productRaw = open("/home/cloudera/Desktop/xyz/src/spark/retail_db/products/part-00000").read().splitlines()
products = sc.parallelize(productRaw)
productsMap = products.map(lambda p: (int(p.split(",")[0]),p.split(",")[2]))

dailyRevenuePerProductMap = dailyRevenuePerProduct.map(lambda o: (o[0][1],(o[0][0],o[1])))
dailyRevenueJoinProduct = dailyRevenuePerProductMap.join(productsMap)

sortDailyRevenueJoinProduct = dailyRevenueJoinProduct.map(lambda o: ((o[1][0][0],-o[1][0][1]),o[1][0][0] + "," + str(o[1][0][1]) + "," + o[1][1]))
dailyRevenuePerProductSorted = sortDailyRevenueJoinProduct.coalesce(1).sortByKey()

dailyRevenuePerProductName = dailyRevenuePerProductSorted.map(lambda f: f[1])

dailyRevenuePerProductName.coalesce(1).saveAsTextFile("/OutputFiles/retail_db/dailyRevenuePerProductName_text")

