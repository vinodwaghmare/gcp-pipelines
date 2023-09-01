select 
date,
channelGrouping,
device.browser,                                  
isEntrance,
page.pagePath
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`
cross join unnest(hits) 
where isEntrance = True

device column is one level deep record/array so we can access it
hits columnn multilevel deep record/array so we need to unnest it

