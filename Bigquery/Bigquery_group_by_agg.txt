# Agg totals with group by,    calculate no. of transactions, visits on channel
  
SELECT 
date,
channelGrouping as channel,
sum(totals.visits) as visits,
sum(totals.transactions) as transactions,
sum(totals.transactionRevenue) as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE channelGrouping in ('Organic Search','Referral')
GROUP BY date, channel
ORDER BY revenue desc
LIMIT 1000


# Writing arthemetic within queries, find the conversion rate, aov & also use case statement while performing divide operation because denominator denominator is zero 
SELECT 
date,
channelGrouping as channel,
sum(totals.visits) as visits,
sum(totals.transactions) as transactions,
sum(totals.transactionRevenue) as revenue,
case when sum(totals.visits) > 0 
  then sum(totals.transactions)  / sum(totals.visits) 
  else 0 end as conv_rate,
case when sum(totals.transactions) > 0 
  then sum(totals.transactionRevenue)  / sum(totals.transactions) 
  else 0 end as aov
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
WHERE channelGrouping in ('Organic Search','Direct')
GROUP BY date, channel
ORDER BY revenue desc
LIMIT 1000







