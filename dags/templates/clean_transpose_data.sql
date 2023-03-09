CREATE TABLE IF NOT EXISTS {{ params.project_id }}.{{ params.dataset }}.{{ params.staging_table }} (
  country string,
  search_term string,
  search_term_interest int64,
  start_date date,
  end_date date
)
PARTITION BY start_date;

INSERT INTO {{ params.project_id }}.{{ params.dataset }}.{{ params.staging_table }} (
  country,
  search_term,
  search_term_interest,
  start_date,
  end_date
)
WITH raw_google_trends_data AS (
SELECT 
  geoName,
  vpn,
  hack,
  cyber,
  security,
  wifi,
  start_date,
  end_date,
  ROW_NUMBER() OVER(PARTITION BY geoName, start_date, end_date ORDER BY batch_date DESC) AS row_n
FROM 
  {{ params.project_id }}.{{ params.dataset }}.{{ params.raw_table }}
),
dedup_data AS (
SELECT 
  geoName,
  vpn,
  hack,
  cyber,
  security,
  wifi,
  start_date,
  end_date
FROM 
  raw_google_trends_data
WHERE row_n = 1
),
data_transpose AS (
SELECT 
  geoName AS country,
  'vpn' AS search_term,
  vpn AS search_term_interest,
  start_date,
  end_date
FROM 
  dedup_data
UNION DISTINCT
SELECT 
  geoName AS country,
  'hack' AS search_term,
  hack AS search_term_interest,
  start_date,
  end_date
FROM 
  dedup_data
UNION DISTINCT
SELECT 
  geoName as country,
  'cyber' as search_term,
  cyber as search_term_interest,
  start_date,
  end_date
FROM 
  dedup_data
UNION DISTINCT
SELECT 
  geoName AS country,
  'security' AS search_term,
  security AS search_term_interest,
  start_date,
  end_date
FROM 
  dedup_data
UNION DISTINCT
SELECT 
  geoName AS country,
  'wifi' AS search_term,
  wifi AS search_term_interest,
  start_date,
  end_date
FROM 
  dedup_data
)
SELECT 
  dt.country,
  dt.search_term,
  dt.search_term_interest,
  dt.start_date,
  dt.end_date
FROM 
  data_transpose AS dt
LEFT JOIN 
  {{ params.project_id }}.{{ params.dataset }}.{{ params.staging_table }} AS fin
ON dt.country = fin.country 
AND dt.search_term = fin.search_term
AND dt.start_date = fin.start_date
AND dt.end_date = fin.end_date
WHERE fin.country IS NULL;