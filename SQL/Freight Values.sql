1. What is freight value correlation to other variables and what is freight value correlation to price?

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

2. What is the average freight value and average price timeline and what is the average freight value and number of orders timeline?
	
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

3. What are freight values number of orders segmented by paid values?

WITH main_data AS (SELECT	
items.freight_value AS freight_value,	
items.price,	
COUNT(items.order_id) AS number_of_orders,	
products.product_weight_g,	
products.product_length_cm,	
products.product_height_cm,	
products.product_width_cm	
FROM `olist_db.olist_order_items_dataset` AS items	
JOIN `olist_db.olist_products_dataset` AS products	
ON items.product_id = products.product_id	
WHERE products.product_weight_g IS NOT NULL	
AND items.freight_value > 0	
	
GROUP BY	
items.freight_value,	
items.price,	
products.product_weight_g,	
products.product_length_cm,	
products.product_height_cm,	
products.product_width_cm	
	
ORDER BY 1 DESC	
)	
	
SELECT	
SUM(CASE WHEN freight_value > 0 AND freight_value < 1 THEN number_of_orders ELSE NULL END) AS _0_to_1,	
SUM(CASE WHEN freight_value > 1 AND freight_value < 5 THEN number_of_orders ELSE NULL END) AS _1_to_5,	
SUM(CASE WHEN freight_value > 5 AND freight_value < 10 THEN number_of_orders ELSE NULL END) AS _5_to_10,	
SUM(CASE WHEN freight_value > 10 AND freight_value < 20 THEN number_of_orders ELSE NULL END) AS _10_to_20,	
SUM(CASE WHEN freight_value > 20 AND freight_value < 50 THEN number_of_orders ELSE NULL END) AS _20_to_50,	
SUM(CASE WHEN freight_value > 50 AND freight_value < 100 THEN number_of_orders ELSE NULL END) AS _50_to_100,	
SUM(CASE WHEN freight_value > 100 AND freight_value < 200 THEN number_of_orders ELSE NULL END) AS _100_to_200,	
SUM(CASE WHEN freight_value > 200 AND freight_value < 300 THEN number_of_orders ELSE NULL END) AS _200_to_300,	
SUM(CASE WHEN freight_value > 300 THEN number_of_orders ELSE NULL END) AS _300_and_more	
	
FROM main_data	
ORDER BY 1	

4. What are freight value number of orders segmented by percentage of order product price?

WITH main_data AS (	
SELECT	
items.freight_value,	
items.price,	
COUNT(items.order_id) AS number_of_orders,	
items.freight_value / items.price AS freight_percentage_of_price	
FROM `olist_db.olist_order_items_dataset` AS items	
JOIN `olist_db.olist_products_dataset` AS products	
ON items.product_id = products.product_id	
WHERE items.freight_value > 0	
GROUP BY	
items.freight_value,	
items.price	
ORDER BY 4 DESC	
)	
SELECT	
SUM(CASE WHEN freight_percentage_of_price > 0.009791918357 AND freight_percentage_of_price	
< 0.04995453168 THEN number_of_orders ELSE NULL END) AS _1_to_5_perc,	
SUM(CASE WHEN freight_percentage_of_price > 0.04995453168 AND freight_percentage_of_price < 0.09995499775 THEN number_of_orders ELSE NULL END) AS _5_to_10_perc,	
SUM(CASE WHEN freight_percentage_of_price > 0.09995499775 AND freight_percentage_of_price < 0.1499593165 THEN number_of_orders ELSE NULL END) AS _10_to_15,	
SUM(CASE WHEN freight_percentage_of_price > 0.1499593165 AND freight_percentage_of_price < 0.24996 THEN number_of_orders ELSE NULL END) AS _15_to_25,	
SUM(CASE WHEN freight_percentage_of_price > 0.24996 AND freight_percentage_of_price < 0.3499615089 THEN number_of_orders ELSE NULL END) AS _25_to_35,	
SUM(CASE WHEN freight_percentage_of_price > 0.3499615089 AND freight_percentage_of_price < 0.5 THEN number_of_orders ELSE NULL END) AS _35_to_50,	
SUM(CASE WHEN freight_percentage_of_price > 0.5 AND freight_percentage_of_price < 0.75 THEN number_of_orders ELSE NULL END) AS _50_to_75,	
SUM(CASE WHEN freight_percentage_of_price > 0.75 AND freight_percentage_of_price < 1.0005 THEN number_of_orders ELSE NULL END) AS _75_to_100,	
SUM(CASE WHEN freight_percentage_of_price > 1.0005 AND freight_percentage_of_price < 2.002597403 THEN number_of_orders ELSE NULL END) AS _100_to_200,	
SUM(CASE WHEN freight_percentage_of_price > 2.002597403 THEN number_of_orders ELSE NULL END) AS _200_and_more	
FROM main_data	

5. What are product categories by revenue - pareto?

WITH top_categories AS (	
	
SELECT	
translation.string_field_1 AS category_name,	
SUM(price + freight_value) AS category_revenue	
	
	
FROM `olist_db.olist_order_items_dataset` AS order_item	
JOIN `olist_db.olist_products_dataset` AS products	
ON order_item.product_id = products.product_id	
JOIN `olist_db.product_category_name_translation` As translation	
ON translation.string_field_0 = products.product_category_name	
	
GROUP BY 1	
ORDER BY 2 DESC	
),	
	
cumulative_revenue AS (	
SELECT	
category_name,	
category_revenue,	
SUM(category_revenue) OVER (ORDER BY category_revenue DESC) AS running_total,	
SUM(category_revenue) OVER () AS total	
	
FROM top_categories	
)	
	
SELECT	
category_name,	
category_revenue,	
running_total,	
total,	
running_total / total AS percent_of_total	
	
	
FROM cumulative_revenue	
	
ORDER BY 2 DESC;	

6. What are the 10 top product categories by revenue freight value over time by category and what is freight value versus number of orders by category?

WITH t1 AS (SELECT
DATE(DATE_TRUNC(orders.order_purchase_timestamp,WEEK)) AS purchase_week,
translation.string_field_1 AS category_name,
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
JOIN `olist_db.product_category_name_translation` As translation
ON translation.string_field_0 = products.product_category_name

WHERE order_status != 'canceled'
AND order_status != 'unavailable'
AND order_purchase_timestamp >= '2017-01-05'


GROUP BY
purchase_week,
category_name

ORDER BY purchase_week ASC)


SELECT *

FROM t1

WHERE category_name = 'health_beauty'
OR category_name = 'watches_gifts'
OR category_name = 'bed_bath_table'
OR category_name = 'sports_leisure'
OR category_name = 'computers_accessories'
OR category_name = 'furniture_decor'
OR category_name = 'housewares'
OR category_name = 'cool_stuff'
OR category_name = 'auto'
OR category_name = 'garden_tools'
OR category_name = 'toys'
OR category_name = 'baby'
OR category_name = 'perfumery'
OR category_name = 'telephony'
OR category_name = 'office_furniture'
OR category_name = 'stationery'

7. What are the average freight value and number of orders by customer states and what are the average freight value and number of orders by sellers states?

WITH full_names AS (
SELECT

AVG(items.freight_value) AS avg_freight_value,
COUNT(items.order_id) AS number_of_orders,
sellers.seller_state,

CASE WHEN sellers.seller_state = 'SP' THEN 'São Paulo'
WHEN sellers.seller_state = 'SE' THEN 'Sergipe'
WHEN sellers.seller_state = 'SC' THEN 'Santa Catarina'
WHEN sellers.seller_state = 'RS' THEN 'Rio Grande do Sul'
WHEN sellers.seller_state = 'RO' THEN 'Rondônia'
WHEN sellers.seller_state = 'RN' THEN 'Rio Grande do Norte'
WHEN sellers.seller_state = 'RJ' THEN 'Rio de Janeiro'
WHEN sellers.seller_state = 'PR' THEN 'Paraná'
WHEN sellers.seller_state = 'PI' THEN 'Piauí'
WHEN sellers.seller_state = 'PE' THEN 'Pernambuco'
WHEN sellers.seller_state = 'PB' THEN 'Paraíba'
WHEN sellers.seller_state = 'PA' THEN 'Pará'
WHEN sellers.seller_state = 'MT' THEN 'MatoGrosso'
WHEN sellers.seller_state = 'MS' THEN 'MatoGrosso do Sul'
WHEN sellers.seller_state = 'MG' THEN 'Minas Gerais'
WHEN sellers.seller_state = 'MA' THEN 'Maranhão'
WHEN sellers.seller_state = 'GO' THEN 'Goiás'
WHEN sellers.seller_state = 'ES' THEN 'Espírito Santo'
WHEN sellers.seller_state = 'DF' THEN 'Distrito Federal'
WHEN sellers.seller_state = 'CE' THEN 'Ceará'
WHEN sellers.seller_state = 'BA' THEN 'Bahia'
WHEN sellers.seller_state = 'AM' THEN 'Amazonas'
END AS seller_state_fullname,

customers.customer_state,

CASE WHEN customers.customer_state = 'TO' THEN 'Tocantins'
WHEN customers.customer_state = 'SP' THEN 'São Paulo'
WHEN customers.customer_state = 'SE' THEN 'Sergipe'
WHEN customers.customer_state = 'SC' THEN 'Santa Catarina'
WHEN customers.customer_state = 'RS' THEN 'Rio Grande do Sul'
WHEN customers.customer_state = 'RR' THEN 'Roraima'
WHEN customers.customer_state = 'RO' THEN 'Rondônia'
WHEN customers.customer_state = 'RN' THEN 'Rio Grande do Norte'
WHEN customers.customer_state = 'RJ' THEN 'Rio de Janeiro'
WHEN customers.customer_state = 'PR' THEN 'Paraná'
WHEN customers.customer_state = 'PI' THEN 'Piauí'
WHEN customers.customer_state = 'PE' THEN 'Pernambuco'
WHEN customers.customer_state = 'PB' THEN 'Paraíba'
WHEN customers.customer_state = 'PA' THEN 'Pará'
WHEN customers.customer_state = 'MT' THEN 'MatoGrosso'
WHEN customers.customer_state = 'MS' THEN 'MatoGrosso do Sul'
WHEN customers.customer_state = 'MG' THEN 'Minas Gerais'
WHEN customers.customer_state = 'MA' THEN 'Maranhão'
WHEN customers.customer_state = 'GO' THEN 'Goiás'
WHEN customers.customer_state = 'ES' THEN 'Espírito Santo'
WHEN customers.customer_state = 'DF' THEN 'Distrito Federal'
WHEN customers.customer_state = 'CE' THEN 'Ceará'
WHEN customers.customer_state = 'BA' THEN 'Bahia'
WHEN customers.customer_state = 'AP' THEN 'Amapá'
WHEN customers.customer_state = 'AM' THEN 'Amazonas'
WHEN customers.customer_state = 'AL' THEN 'Alagoas'
WHEN customers.customer_state = 'AC' THEN 'Acre'

END AS customer_state_fullname

FROM `olist_db.olist_orders_dataset` AS orders
JOIN `olist_db.olist_customesr_dataset` AS customers
ON orders.customer_id = customers.customer_id
JOIN `olist_db.olist_order_items_dataset` AS items
ON orders.order_id = items.order_id
JOIN `olist_db.olist_sellers_dataset` AS sellers
ON items.seller_id = sellers.seller_id

WHERE
orders.order_purchase_timestamp >= '2017-01-01'

GROUP BY
sellers.seller_state,
customers.customer_state,
seller_state_fullname,
customer_state_fullname

)

SELECT

avg_freight_value,
number_of_orders,
customer_state_fullname

FROM full_names

WHERE customer_state_fullname = 'Rio Grande do Sul'

ORDER BY 1 DESC
