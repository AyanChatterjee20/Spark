Here we are working with particulary 3 data set **orders** , **order_items** and **products**. 

Please find the below field description of the dataset ***orders*** :

| Field| Type|
|---|---|
| order_id| int(11)|
| order_date|datetime|
| order_customer_id|int(11)|
| order_status|varchar(45)|

Details of dataset **orders** below : 
```
[cloudera@quickstart ~]$ hdfs dfs -cat /InputFiles/retail_db/orders/part-00000 | head
1,2013-07-25 00:00:00.0,11599,CLOSED
2,2013-07-25 00:00:00.0,256,PENDING_PAYMENT
3,2013-07-25 00:00:00.0,12111,COMPLETE
4,2013-07-25 00:00:00.0,8827,CLOSED
5,2013-07-25 00:00:00.0,11318,COMPLETE
6,2013-07-25 00:00:00.0,7130,COMPLETE
7,2013-07-25 00:00:00.0,4530,COMPLETE
8,2013-07-25 00:00:00.0,2911,PROCESSING
9,2013-07-25 00:00:00.0,5657,PENDING_PAYMENT
10,2013-07-25 00:00:00.0,5648,PENDING_PAYMENT
```

Please find the below field description of the dataset ***order_items*** :

| Field | Type|
|---|---|
| order_item_id | int(11)|
| order_item_order_id| int(11)|
| order_item_product_id| int(11)|
| order_item_quantity| tinyint(4)|
| order_item_subtotal| float|
| order_item_product_price| float|

Details of dataset **orders** below :
```
[cloudera@quickstart ~]$ hdfs dfs -cat /InputFiles/retail_db/order_items/part-00000 | head
1,1,957,1,299.98,299.98
2,2,1073,1,199.99,199.99
3,2,502,5,250.0,50.0
4,2,403,1,129.99,129.99
5,4,897,2,49.98,24.99
6,4,365,5,299.95,59.99
7,4,502,3,150.0,50.0
8,4,1014,4,199.92,49.98
9,5,957,1,299.98,299.98
10,5,365,5,299.95,59.99
```

Please find the below field description of the dataset ***products*** :

| Field               | Type         |
|---|---|
| product_id          | int(11)      |
| product_category_id | int(11)      |
| product_name        | varchar(45)  |
| product_description | varchar(255) |
| product_price       | float        |
| product_image       | varchar(255) |
