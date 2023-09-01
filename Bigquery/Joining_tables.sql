
SELECT 
zip.state_code ,
sum(census.population) population
FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010` census
LEFT JOIN
`bigquery-public-data.geo_us_boundaries.zip_codes`  zip
on census.zipcode = zip.zip_code
where census.maximum_age is null
and census.minimum_age is null
and census.gender is null
group by zip.state_code
limit 1000
