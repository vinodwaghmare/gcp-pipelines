SELECT 
date,
channelGrouping,
pageviews,
round((pageviews / sum(pageviews) over w1 ) * 100.0, 2) as percent_views_per_channel,
avg(pageviews) over w1 as avg_pageviews,
FROM
(
    SELECT 
    date,
    channelGrouping,
    sum(totals.pageviews) as pageviews
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
    group by channelGrouping, date
)
window w1 as (PARTITION BY date)
ORDER BY percent_views_per_channel desc

