1. All campaign session data without bounced sessions

WITH last_date AS (
SELECT user_pseudo_id,
event_timestamp,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_date,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS last_event,
event_name,
campaign,
category,
country
FROM `turing_data_analytics.raw_events`
),

new_session AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
last_event,
CASE WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event),SECOND)
>= (60*30) OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
event_name,
campaign,
category,
country

FROM last_date
),

final AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(last_event), SECOND) AS last_event,
is_new_session,
SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS global_session_id,
CONCAT(user_pseudo_id, '_', SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)) AS user_session_id,
event_name,
campaign,
category,
country

FROM new_session
)

-- all campaigns with removed bounce sessions

SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country,
TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)),SECOND) AS session_duration_in_sec

FROM final

WHERE campaign IN ('BlackFriday_V1','BlackFriday_V2','NewYear_V1','NewYear_V2','Holiday_V1','Holiday_V2','Data Share Promo')


GROUP BY

user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country

HAVING session_duration_in_sec > 0

ORDER BY global_session_id ASC

2. All campaign session event numbers without bounced sessions

WITH last_date AS (
SELECT user_pseudo_id,
event_timestamp,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_date,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS last_event,
event_name,
campaign,
category,
country
FROM `turing_data_analytics.raw_events`
),

new_session AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
last_event,
CASE WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event),SECOND)
>= (60*30) OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
event_name,
campaign,
category,
country

FROM last_date
),

final AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(last_event), SECOND) AS last_event,
is_new_session,
SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS global_session_id,
CONCAT(user_pseudo_id, '_', SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)) AS user_session_id,
event_name,
campaign,
category,
country

FROM new_session
)


-- all campaigns and non-campaigns without bounces to find the number of events per campaigns

SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
COUNT(event_name) AS event_number,
TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)),SECOND) AS session_duration_in_sec,
category,
country

FROM final

WHERE campaign IN ('BlackFriday_V1','BlackFriday_V2','NewYear_V1','NewYear_V2','Holiday_V1','Holiday_V2','Data Share Promo')

GROUP BY

user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country

HAVING session_duration_in_sec > 5

ORDER BY global_session_id ASC

3. All campaigns bounce rate

WITH last_date AS (
SELECT user_pseudo_id,
event_timestamp,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_date,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS last_event,
event_name,
campaign,
category,
country
FROM `turing_data_analytics.raw_events`
),

new_session AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
last_event,
CASE WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event),SECOND)
>= (60*30) OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
event_name,
campaign,
category,
country

FROM last_date
),

final AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(last_event), SECOND) AS last_event,
is_new_session,
SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS global_session_id,
CONCAT(user_pseudo_id, '_', SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)) AS user_session_id,
event_name,
campaign,
category,
country

FROM new_session
),


-- all campaigns and non campaigns with bounces to find number of event per campaigns

all_campaigns AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
COUNT(event_name) AS event_number,
TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)),SECOND) AS session_duration_in_sec,
category,
country

FROM final

GROUP BY

user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country

ORDER BY global_session_id ASC),

bounce_rate AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
CASE WHEN campaign = 'BlackFriday_V1' THEN 1
WHEN campaign = 'BlackFriday_V2' THEN 1
WHEN campaign = 'NewYear_V1' THEN 1
WHEN campaign = 'NewYear_V2' THEN 1
WHEN campaign = 'Holiday_V1' THEN 1
WHEN campaign = 'Holiday_V2' THEN 1
WHEN campaign = 'Holiday_V1' THEN 1
WHEN campaign = 'Data Share Promo' THEN 1
ELSE 0 END AS paid_channels,

CASE WHEN session_duration_in_sec < 5 THEN 1
ELSE 0 END AS customer_bounced,
session_duration_in_sec,
category,
country

FROM all_campaigns

WHERE campaign IS NOT NULL

ORDER BY global_session_id ASC)

SELECT

COUNT(paid_channels) AS non_paid_sessions,
SUM(customer_bounced) AS non_paid_session_bounced,
SUM(paid_channels) AS paid_sessions,
COUNT(CASE WHEN paid_channels = 1 AND customer_bounced = 1 THEN customer_bounced END) AS paid_session_bounced


FROM bounce_rate

4. Paid & non-paid Session data comparison in total values

WITH last_date AS (
SELECT user_pseudo_id,
event_timestamp,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_date,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS last_event,
event_name,
campaign,
category,
country
FROM `turing_data_analytics.raw_events`
),

new_session AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
last_event,
CASE WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event),SECOND)
>= (60*30) OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
event_name,
campaign,
category,
country

FROM last_date
),

final AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(last_event), SECOND) AS last_event,
is_new_session,
SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS global_session_id,
CONCAT(user_pseudo_id, '_', SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)) AS user_session_id,
event_name,
campaign,
category,
country

FROM new_session
),


-- all campaigns and non campaigns with bounces to find number of event per campaigns

all_campaigns AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
COUNT(event_name) AS event_number,
TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)),SECOND) AS session_duration_in_sec,
category,
country

FROM final

GROUP BY

user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country

ORDER BY global_session_id ASC),

-- -- all campaigns without bounced customers to set up session type (paid vs non paid)

paid_non_paid AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
event_number,
CASE WHEN campaign = 'BlackFriday_V1' THEN 'paid'
WHEN campaign = 'BlackFriday_V2' THEN 'paid'
WHEN campaign = 'NewYear_V1' THEN 'paid'
WHEN campaign = 'NewYear_V2' THEN 'paid'
WHEN campaign = 'Holiday_V1' THEN 'paid'
WHEN campaign = 'Holiday_V2' THEN 'paid'
WHEN campaign = 'Holiday_V1' THEN 'paid'
WHEN campaign = 'Data Share Promo' THEN 'paid'
ELSE 'non paid' END AS session_type,

CASE WHEN session_duration_in_sec < 5 THEN 1
ELSE 0 END AS customer_bounced,
session_duration_in_sec,
category,
country

FROM all_campaigns

WHERE campaign IS NOT NULL

ORDER BY global_session_id ASC)

-- compare paid and non paid total session time and event number

SELECT
week_day,
SUM(CASE WHEN campaign = 'BlackFriday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'BlackFriday_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'NewYear_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'NewYear_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Data Share Promo' AND session_type = 'paid' THEN event_number
END) AS paid_event_number,

SUM(CASE WHEN campaign = 'BlackFriday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'BlackFriday_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'NewYear_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'NewYear_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Data Share Promo' AND session_type = 'paid' THEN session_duration_in_sec
END) AS paid_total_session_duration_in_sec,

SUM(CASE WHEN campaign = '(referral)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '<Other>' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(organic)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(direct)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(data deleted)' AND session_type = 'non paid' THEN event_number
END) AS non_paid_event_number,

SUM(CASE WHEN campaign = '(referral)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '<Other>' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(organic)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(direct)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(data deleted)' AND session_type = 'non paid' THEN session_duration_in_sec
END) AS non_paid_total_session_duration_in_sec

FROM paid_non_paid

GROUP BY 1

ORDER BY 1

5. Paid & non-paid Session data comparison in average values

WITH last_date AS (
SELECT user_pseudo_id,
event_timestamp,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(event_timestamp), SECOND) AS event_date,
LAG(event_timestamp,1) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS last_event,
event_name,
campaign,
category,
country
FROM `turing_data_analytics.raw_events`
),

new_session AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
last_event,
CASE WHEN TIMESTAMP_DIFF(TIMESTAMP_MICROS(event_timestamp), TIMESTAMP_MICROS(last_event),SECOND)
>= (60*30) OR last_event IS NULL THEN 1 ELSE 0 END AS is_new_session,
event_name,
campaign,
category,
country

FROM last_date
),

final AS (
SELECT
user_pseudo_id,
event_timestamp,
event_date,
FORMAT_DATE("%A",DATE_TRUNC(event_date, DAY)) AS week_day,
TIMESTAMP_TRUNC(TIMESTAMP_MICROS(last_event), SECOND) AS last_event,
is_new_session,
SUM(is_new_session) OVER (ORDER BY user_pseudo_id, event_timestamp) AS global_session_id,
CONCAT(user_pseudo_id, '_', SUM(is_new_session) OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp)) AS user_session_id,
event_name,
campaign,
category,
country

FROM new_session
),


-- all campaigns and non campaigns with bounces to find number of event per campaigns

all_campaigns AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
COUNT(event_name) AS event_number,
TIMESTAMP_DIFF(MAX(TIMESTAMP_MICROS(event_timestamp)), MIN(TIMESTAMP_MICROS(event_timestamp)),SECOND) AS session_duration_in_sec,
category,
country

FROM final

GROUP BY

user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
category,
country

ORDER BY global_session_id ASC),

-- -- all campaigns without bounced customers to set up session type (paid vs non paid)

paid_non_paid AS (SELECT
user_pseudo_id,
global_session_id,
user_session_id,
week_day,
campaign,
event_number,
CASE WHEN campaign = 'BlackFriday_V1' THEN 'paid'
WHEN campaign = 'BlackFriday_V2' THEN 'paid'
WHEN campaign = 'NewYear_V1' THEN 'paid'
WHEN campaign = 'NewYear_V2' THEN 'paid'
WHEN campaign = 'Holiday_V1' THEN 'paid'
WHEN campaign = 'Holiday_V2' THEN 'paid'
WHEN campaign = 'Holiday_V1' THEN 'paid'
WHEN campaign = 'Data Share Promo' THEN 'paid'
ELSE 'non paid' END AS session_type,

CASE WHEN session_duration_in_sec < 5 THEN 1
ELSE 0 END AS customer_bounced,
session_duration_in_sec,
category,
country

FROM all_campaigns

WHERE campaign IS NOT NULL

ORDER BY global_session_id ASC)

-- compare paid and non paid average session time and average event number

SELECT
week_day,
AVG(CASE WHEN campaign = 'BlackFriday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'BlackFriday_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'NewYear_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'NewYear_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V2' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN event_number
WHEN campaign = 'Data Share Promo' AND session_type = 'paid' THEN event_number
END) AS paid_event_number,

AVG(CASE WHEN campaign = 'BlackFriday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'BlackFriday_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'NewYear_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'NewYear_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V2' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Holiday_V1' AND session_type = 'paid' THEN session_duration_in_sec
WHEN campaign = 'Data Share Promo' AND session_type = 'paid' THEN session_duration_in_sec
END) AS paid_total_session_duration_in_sec,

AVG(CASE WHEN campaign = '(referral)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '<Other>' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(organic)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(direct)' AND session_type = 'non paid' THEN event_number
WHEN campaign = '(data deleted)' AND session_type = 'non paid' THEN event_number
END) AS non_paid_event_number,

AVG(CASE WHEN campaign = '(referral)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '<Other>' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(organic)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(direct)' AND session_type = 'non paid' THEN session_duration_in_sec
WHEN campaign = '(data deleted)' AND session_type = 'non paid' THEN session_duration_in_sec
END) AS non_paid_total_session_duration_in_sec

FROM paid_non_paid

GROUP BY 1

ORDER BY 1

