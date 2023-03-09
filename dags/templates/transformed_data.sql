CREATE TABLE IF NOT EXISTS {{ params.project_id }}.{{ params.dataset }}.{{ params.final_table }} (
  country string,
  search_term string,
  search_term_interest int64,
  start_date date,
  end_date date,
  rank int64
)
PARTITION BY start_date;

INSERT INTO {{ params.project_id }}.{{ params.dataset }}.{{ params.final_table }} (
  country,
  search_term,
  search_term_interest,
  start_date,
  end_date,
  rank
)
WITH change_vpn_name AS (
SELECT 
  country,
  REPLACE(search_term, 'vpn', '~~~~||vpn') AS search_term,
  search_term_interest,
  start_date,
  end_date
FROM 
  {{ params.project_id }}.{{ params.dataset }}.{{ params.staging_table }}
),
rankings AS (
SELECT 
  country,
  REPLACE(search_term, '~~~~||vpn', 'vpn') AS search_term,
  search_term_interest,
  start_date,
  end_date,
  ROW_NUMBER() OVER(PARTITION BY country, start_date, end_date ORDER BY search_term_interest DESC, search_term) AS rank
FROM 
  change_vpn_name
)
SELECT 
  cn.country,
  cn.search_term,
  cn.search_term_interest,
  cn.start_date,
  cn.end_date,
  cn.rank
FROM 
  rankings AS cn
LEFT JOIN 
  {{ params.project_id }}.{{ params.dataset }}.{{ params.final_table }} AS fin 
ON cn.country = fin.country
AND cn.search_term = fin.search_term
AND cn.start_date = fin.start_date
AND cn.end_date = fin.end_date
WHERE fin.country IS NULL;
