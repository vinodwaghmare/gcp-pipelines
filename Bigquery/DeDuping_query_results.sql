select 
*
from 
(
  SELECT 
date,
channelGrouping,
totals.hits,
visitStartTime,
first_value(visitStartTime) over (PARTITION BY channelGrouping order by visitStartTime desc) as lv
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
)
where visitStartTime = lv
order by channelGrouping asc

# here we are just getting the latest time when the site is visited across channel groups
