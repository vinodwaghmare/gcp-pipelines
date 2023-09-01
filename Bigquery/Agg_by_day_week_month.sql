SELECT 
date,
extract(day from date) day_of_month ,
extract(week from date) week_of_the_year,
FORMAT_DATE('%Y-%m', date) as yyyymm
FROM
(
  SELECT
  PARSE_DATE('%Y%m%d',date) as date,
  channelGrouping,
  totals.visits,
  totals.transactions,
  totals.transactionRevenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
)

PARSE_DATE() function is used to convert string into date datatype 
FORMAT_DATE() function is used to format date into required formate
