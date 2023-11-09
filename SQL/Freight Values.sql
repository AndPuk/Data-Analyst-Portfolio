1. What is freight value correlation to other variables?
   What freight value correlation to price?

SELECT	
items.freight_value,	
items.price,	
products.product_weight_g,	
products.product_length_cm,	
products.product_height_cm,	
products.product_width_cm	
	
FROM `olist_db.olist_order_items_dataset` AS items	
JOIN `olist_db.olist_products_dataset` AS products	
ON items.product_id = products.product_id	
	
WHERE products.product_weight_g IS NOT NULL	
AND items.freight_value > 0	

2. What is average freight values and average price timeline?
   What is average freight value and number of orders timeline?
	
SELECT	
DATE(DATE_TRUNC(orders.order_purchase_timestamp,WEEK)) AS purchase_week,	
COUNT(orders.order_id) AS number_of_orders,	
SUM(payments.payment_value)/COUNT(orders.order_id) AS avg_order_value,	
AVG(items.freight_value) AS avg_freight_value,	
AVG(items.price) AS avg_price,	
AVG(products.product_weight_g) AS avg_weight_g	
	
	
	
FROM `olist_db.olist_products_dataset` AS products	
JOIN `olist_db.olist_order_items_dataset` AS items	
ON products.product_id = items.product_id	
JOIN `olist_db.olist_orders_dataset` AS orders	
ON items.order_id = orders.order_id	
JOIN `olist_db.olist_order_payments_dataset` AS payments	
ON orders.order_id = payments.order_id	
WHERE order_status != 'canceled'	
AND order_status != 'unavailable'	
AND order_purchase_timestamp >= '2017-01-05'	
GROUP BY	
	
purchase_week	
ORDER BY purchase_week ASC;	
