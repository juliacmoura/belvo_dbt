with population as (
 
SELECT
    year,
    country_code,
    IF(country_name = "United States","US",IF(country_name="Iran, Islamic Rep.","Iran",country_name)) as country_name,
    sum(population) as population
  FROM
    `bigquery-public-data.census_bureau_international.midyear_population_agespecific`
  WHERE
    year in (2020,2021,2022)
  GROUP BY 1,2,3

),

covid_cases as (

SELECT
  extract(year from date) as year,
  date,
  country_region,
  SUM(confirmed) AS confirmed_cases
FROM
  `bigquery-public-data.covid19_jhu_csse.summary`  

GROUP BY 1,2,3
 
)

select 
covid_cases.country_region as country,
extract (week from covid_cases.date) as week,
covid_cases.date,
covid_cases.confirmed_cases,
population.population,
covid_cases.confirmed_cases / population.population as cases_per_capta
from covid_cases left join population on (covid_cases.country_region =  population.country_name and covid_cases.year =  population.year)
