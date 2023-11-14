1. New and returning customer purchase duration revenue

WITH first_visit AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
user_pseudo_id,
MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_visit_date
FROM `turing_data_analytics.raw_events`
GROUP BY 1,2
),

purchase AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
TIMESTAMP_MICROS(event_timestamp) AS purchase_date,
user_pseudo_id,
event_name,
purchase_revenue_in_usd
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
)

SELECT
first_visit.date,
first_visit.user_pseudo_id,
TIMESTAMP_DIFF(purchase.purchase_date, first_visit.first_visit_date, MINUTE) AS time_spent_min,
purchase.purchase_revenue_in_usd AS revenue_usd
FROM first_visit
JOIN purchase
ON first_visit.user_pseudo_id = purchase.user_pseudo_id

WHERE first_visit.date = purchase.date
ORDER BY 1


-- Returning customer's duration from first arriving on the website on any given day until their first purchase on any other day and revenues generated

WITH first_visit AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
user_pseudo_id,
TIMESTAMP_MICROS(event_timestamp) AS first_visit_date
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'first_visit'

),

purchase AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
TIMESTAMP_MICROS(event_timestamp) AS purchase_date,
user_pseudo_id,
event_name,
purchase_revenue_in_usd
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
)

SELECT
first_visit.date,
first_visit.user_pseudo_id,
TIMESTAMP_DIFF(purchase.purchase_date, first_visit.first_visit_date, MINUTE) AS time_spent_min,
purchase.purchase_revenue_in_usd AS revenue_usd
FROM first_visit
JOIN purchase
ON first_visit.user_pseudo_id = purchase.user_pseudo_id

WHERE first_visit.date != purchase.date
ORDER BY 1

2. New customer total time duration until purchase by device

WITH first_visit AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
user_pseudo_id,
category,
MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_visit_date
FROM `turing_data_analytics.raw_events`
GROUP BY 1,2,3
),

purchase AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
TIMESTAMP_MICROS(event_timestamp) AS purchase_date,
category,
user_pseudo_id,
event_name
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
)

SELECT
purchase.date,
first_visit.user_pseudo_id,
TIMESTAMP_DIFF(purchase.purchase_date, first_visit.first_visit_date, MINUTE) AS time_spent_min,
first_visit.category

FROM first_visit
JOIN purchase
ON first_visit.user_pseudo_id = purchase.user_pseudo_id

WHERE first_visit.date = purchase.date
AND first_visit.category = purchase.category

ORDER BY 1

3. New customer time duration until purchase by sessions

WITH first_visit AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
user_pseudo_id,
MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_visit_date
FROM `turing_data_analytics.raw_events`
GROUP BY 1,2

),

purchase AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
TIMESTAMP_MICROS(event_timestamp) AS purchase_date,
user_pseudo_id,
event_name,
purchase_revenue_in_usd
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
),

total_time AS (
SELECT
purchase.date AS timeline,
first_visit.user_pseudo_id AS user_id,
TIMESTAMP_DIFF(purchase.purchase_date, first_visit.first_visit_date, MINUTE) AS time_spent_min,
purchase.purchase_revenue_in_usd AS revenue_usd
FROM first_visit
JOIN purchase
ON first_visit.user_pseudo_id = purchase.user_pseudo_id
WHERE first_visit.date = purchase.date

)

SELECT
total_time.timeline,
total_time.user_id,
total_time.time_spent_min,
total_time.revenue_usd,
CASE WHEN total_time.time_spent_min > 0 AND total_time.time_spent_min < 5 THEN total_time.revenue_usd ELSE NULL END AS _0min_to_5min,
CASE WHEN total_time.time_spent_min > 5 AND total_time.time_spent_min < 15 THEN total_time.revenue_usd ELSE NULL END AS _5min_to_15min,
CASE WHEN total_time.time_spent_min > 15 AND total_time.time_spent_min < 30 THEN total_time.revenue_usd ELSE NULL END AS _15min_to_30min,
CASE WHEN total_time.time_spent_min > 30 AND total_time.time_spent_min < 60 THEN total_time.revenue_usd ELSE NULL END AS _30min_to_1hour,
CASE WHEN total_time.time_spent_min > 60 AND total_time.time_spent_min < 120 THEN total_time.revenue_usd ELSE NULL END AS _1hour_to_2hours,
CASE WHEN total_time.time_spent_min > 120 AND total_time.time_spent_min < 180 THEN total_time.revenue_usd ELSE NULL END AS _2hours_to_3hours,
CASE WHEN total_time.time_spent_min > 180 AND total_time.time_spent_min < 300 THEN total_time.revenue_usd ELSE NULL END AS _3hours_to_5hours,
CASE WHEN total_time.time_spent_min > 300 AND total_time.time_spent_min < 720 THEN total_time.revenue_usd ELSE NULL END AS _5hours_to_12hours,
CASE WHEN total_time.time_spent_min > 720 AND total_time.time_spent_min < 1440 THEN total_time.revenue_usd ELSE NULL END AS _12hours_to_24hours

FROM total_time
ORDER BY 1

4. New customer time duration until purchase by Sessions & Device

WITH first_visit AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
user_pseudo_id,
category,
MIN(TIMESTAMP_MICROS(event_timestamp)) AS first_visit_date
FROM `turing_data_analytics.raw_events`
GROUP BY 1,2,3

),

purchase AS (
SELECT
PARSE_DATE('%Y%m%d',event_date) AS date,
TIMESTAMP_MICROS(event_timestamp) AS purchase_date,
user_pseudo_id,
category,
event_name,
purchase_revenue_in_usd
FROM `turing_data_analytics.raw_events`
WHERE event_name = 'purchase'
),

total_time AS (
SELECT
purchase.date AS timeline,
first_visit.user_pseudo_id AS user_id,
first_visit.category AS devices,
TIMESTAMP_DIFF(purchase.purchase_date, first_visit.first_visit_date, MINUTE) AS time_spent_min,
purchase.purchase_revenue_in_usd AS revenue_usd
FROM first_visit
JOIN purchase
ON first_visit.user_pseudo_id = purchase.user_pseudo_id

WHERE first_visit.date = purchase.date
AND first_visit.category = purchase.category

)

SELECT
total_time.timeline,
total_time.user_id,
total_time.time_spent_min,
total_time.revenue_usd,
CASE WHEN total_time.devices = 'desktop' THEN 1 ELSE 0 END AS desktop,
CASE WHEN total_time.devices = 'mobile' THEN 1 ELSE 0 END + CASE WHEN total_time.devices = 'tablet' THEN 1 ELSE 0 END AS mobile,
CASE WHEN total_time.time_spent_min > 0 AND total_time.time_spent_min < 5 THEN total_time.revenue_usd ELSE NULL END AS _0min_to_5min,
CASE WHEN total_time.time_spent_min > 5 AND total_time.time_spent_min < 15 THEN total_time.revenue_usd ELSE NULL END AS _5min_to_15min,
CASE WHEN total_time.time_spent_min > 15 AND total_time.time_spent_min < 30 THEN total_time.revenue_usd ELSE NULL END AS _15min_to_30min,
CASE WHEN total_time.time_spent_min > 30 AND total_time.time_spent_min < 60 THEN total_time.revenue_usd ELSE NULL END AS _30min_to_1hour,
CASE WHEN total_time.time_spent_min > 60 AND total_time.time_spent_min < 120 THEN total_time.revenue_usd ELSE NULL END AS _1hour_to_2hours,
CASE WHEN total_time.time_spent_min > 120 AND total_time.time_spent_min < 180 THEN total_time.revenue_usd ELSE NULL END AS _2hours_to_3hours,
CASE WHEN total_time.time_spent_min > 180 AND total_time.time_spent_min < 300 THEN total_time.revenue_usd ELSE NULL END AS _3hours_to_5hours,
CASE WHEN total_time.time_spent_min > 300 AND total_time.time_spent_min < 720 THEN total_time.revenue_usd ELSE NULL END AS _5hours_to_12hours,
CASE WHEN total_time.time_spent_min > 720 AND total_time.time_spent_min < 1440 THEN total_time.revenue_usd ELSE NULL END AS _12hours_to_24hours

FROM total_time

WHERE CASE WHEN total_time.devices = 'mobile' THEN 1 ELSE 0 END + CASE WHEN total_time.devices = 'tablet' THEN 1 ELSE 0 END = 1

ORDER BY 1


