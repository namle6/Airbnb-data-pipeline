-- Create the raw listings table
CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.listings_raw` AS
SELECT 
  Title,
  Detail,
  Date,
  REPLACE(REPLACE(`Price(in dollar)`, '$', ''), ',', '') AS Price_in_dollar,
  REPLACE(REPLACE(`Offer price(in dollar)`, '$', ''), ',', '') AS Offer_price,
  `Review and rating` AS Review_and_rating,
  `Number of bed` AS Number_of_bed
FROM 
  `airbnb-hackathon-project.airbnb_value.raw_import`;

-- Calculate property values
CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.property_values` AS
SELECT
  *,
  -- Extract numeric price, removing non-numeric characters
  SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) AS price_numeric,
  
  -- Categorize properties
  CASE
    WHEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) > 5000 THEN 'luxury'
    WHEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) > 1000 THEN 'premium'
    ELSE 'regular'
  END AS property_category,
  
  -- Calculate property value using income approach:
  -- Annual Revenue / Cap Rate, where:
  -- Annual Revenue = Daily Rate × 365 × Occupancy Rate
  -- Cap Rate varies by property type
  CASE
    -- Luxury properties: 60% occupancy, 3.5% cap rate
    WHEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) > 5000 
      THEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.60 / 0.035
    -- Premium properties: 55% occupancy, 4.5% cap rate
    WHEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) > 1000 
      THEN SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.55 / 0.045
    -- Regular properties: 65% occupancy, 5% cap rate
    ELSE SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.65 / 0.05
  END AS estimated_property_value
FROM 
  `airbnb-hackathon-project.airbnb_value.listings_raw`
WHERE
  SAFE_CAST(REGEXP_REPLACE(Price_in_dollar, r'[^0-9\.]', '') AS FLOAT64) > 0;

-- Detailed Property Category Analysis
CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.property_category_analysis` AS
SELECT
  property_category,
  COUNT(*) AS category_count,
  ROUND(AVG(price_numeric), 2) AS avg_daily_rate,
  ROUND(AVG(estimated_property_value), 2) AS avg_property_value,
  ROUND(MIN(estimated_property_value), 2) AS min_property_value,
  ROUND(MAX(estimated_property_value), 2) AS max_property_value,
  ROUND(SUM(estimated_property_value), 2) AS total_category_value
FROM 
  `airbnb-hackathon-project.airbnb_value.property_values`
GROUP BY 
  property_category;

-- Global Estimate Query
CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.global_airbnb_estimate` AS
SELECT
  COUNT(*) AS sample_size,
  ROUND(AVG(price_numeric), 2) AS avg_daily_rate,
  ROUND(AVG(estimated_property_value), 2) AS avg_property_value,
  -- Airbnb global listing count (7 million)
  7000000 AS estimated_global_listings,
  -- Total global value
  ROUND(7000000 * AVG(estimated_property_value), 2) AS estimated_total_value,
  -- Format as trillions for readability
  FORMAT("$%.2f trillion", 7000000 * AVG(estimated_property_value) / 1000000000000) AS total_value_trillions
FROM 
  `airbnb-hackathon-project.airbnb_value.property_values`;

-- Location-based Analysis
CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.location_value_analysis` AS
WITH LocationAnalysis AS (
  SELECT
    REGEXP_EXTRACT(Detail, r'in ([^,]+),') AS location,
    COUNT(*) AS listing_count,
    ROUND(AVG(price_numeric), 2) AS avg_daily_rate,
    ROUND(AVG(estimated_property_value), 2) AS avg_property_value,
    ROUND(SUM(estimated_property_value), 2) AS total_location_value
  FROM 
    `airbnb-hackathon-project.airbnb_value.property_values`
  GROUP BY 
    location
)
SELECT 
  *,
  RANK() OVER (ORDER BY total_location_value DESC) AS location_value_rank
FROM 
  LocationAnalysis
WHERE 
  listing_count > 10
ORDER BY 
  total_location_value DESC
LIMIT 50;

-- Final Summary View
CREATE OR REPLACE VIEW `airbnb-hackathon-project.airbnb_value.airbnb_valuation_summary` AS
SELECT 
  (SELECT total_value_trillions FROM `airbnb-hackathon-project.airbnb_value.global_airbnb_estimate`) AS global_total_value,
  (SELECT ROUND(AVG(avg_property_value), 2) FROM `airbnb-hackathon-project.airbnb_value.property_category_analysis`) AS avg_property_value,
  (SELECT SUM(category_count) FROM `airbnb-hackathon-project.airbnb_value.property_category_analysis`) AS total_listings,
  (SELECT ROUND(SUM(total_category_value), 2) FROM `airbnb-hackathon-project.airbnb_value.property_category_analysis`) AS total_portfolio_value;