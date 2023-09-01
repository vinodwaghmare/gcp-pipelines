with census as (
  SELECT
  zipcode,
  population
  FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010` 
  where maximum_age is null
  and minimum_age is null
  and gender is null
),

zip as (
  SELECT
  zip_code,
  state_code
  FROM `bigquery-public-data.geo_us_boundaries.zip_codes` 
) 

SELECT 
state_code ,
sum(population) population
FROM census LEFT JOIN zip
on census.zipcode = zip.zip_code
group by state_code
order by population desc
limit 1000
