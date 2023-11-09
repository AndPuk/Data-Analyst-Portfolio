1. Freight value correlation to other variables and freight value correlation to price

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
