# Airbnb Global Real Estate Valuation

## Project Overview
This project estimates the total value of real estate platformed by Airbnb globally, created for the QVIDTVM x The Ion Hack Day Competition. The approach uses data from various Airbnb datasets and applies real estate valuation methodologies to calculate the underlying property values.

## Methodology

### Data Collection
The project uses several datasets provided for the hackathon:
- 2023 USA Airbnb Dataset
- 2024 40 City Asia Dataset
- Airbnb Europe Dataset
- Airbnb Global Dataset
- Specialized datasets for desert and luxury properties

### Valuation Approach
The project employs the income capitalization approach to value properties:
- **Property Value = Annual Revenue / Cap Rate**
- Where Annual Revenue = Daily Rate × 365 × Occupancy Rate

The model uses different parameters based on property type:
1. **Luxury Properties** (price > $5,000/night)
   - Occupancy Rate: 60%
   - Cap Rate: 3.5%

2. **Premium Properties** (price $1,000-$5,000/night)
   - Occupancy Rate: 55%
   - Cap Rate: 4.5%

3. **Regular Properties** (price < $1,000/night)
   - Occupancy Rate: 65%
   - Cap Rate: 5.0%

### Technical Implementation
The solution is implemented using Google Cloud Platform (GCP) with the following components:
- **Google Cloud Storage**: For storing raw CSV data files
- **BigQuery**: For data processing and analysis
- **SQL Queries**: For data transformation and valuation calculations

## How to Run the Analysis

1. **Upload Data to Cloud Storage**
   ```
   gsutil -m cp ./data/*.csv gs://airbnb-raw-data/
   ```

2. **Create BigQuery Dataset**
   ```sql
   CREATE DATASET `airbnb_value`;
   ```

3. **Create Raw Data Table**
   - Create a table from the CSV files stored in Google Cloud Storage
   - Table name: `simple_listings`
   - Auto-detect schema

4. **Run Property Valuation Query**
   ```sql
   -- Calculate property values
   CREATE OR REPLACE TABLE `airbnb-hackathon-project.airbnb_value.property_values` AS
   SELECT
     *,
     -- Extract numeric price, removing non-numeric characters
     SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) AS price_numeric,
     
     -- Categorize properties
     CASE
       WHEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) > 5000 THEN 'luxury'
       WHEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) > 1000 THEN 'premium'
       ELSE 'regular'
     END AS property_category,
     
     -- Calculate property value using income approach:
     -- Annual Revenue / Cap Rate, where:
     -- Annual Revenue = Daily Rate × 365 × Occupancy Rate
     -- Cap Rate varies by property type
     CASE
       -- Luxury properties: 60% occupancy, 3.5% cap rate
       WHEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) > 5000 
         THEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.60 / 0.035
       -- Premium properties: 55% occupancy, 4.5% cap rate
       WHEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) > 1000 
         THEN SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.55 / 0.045
       -- Regular properties: 65% occupancy, 5% cap rate
       ELSE SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) * 365 * 0.65 / 0.05
     END AS estimated_property_value
   FROM 
     `airbnb-hackathon-project.airbnb_value.simple_listings`
   WHERE
     SAFE_CAST(REGEXP_REPLACE(`Price(in dollar)`, r'[^0-9\.]', '') AS FLOAT64) > 0;
   ```

5. **Run Global Estimation Query**
   ```sql
   -- Calculate global estimate
   SELECT
     COUNT(*) AS sample_size,
     AVG(price_numeric) AS avg_daily_rate,
     AVG(estimated_property_value) AS avg_property_value,
     -- Airbnb global listing count (7 million)
     7000000 AS estimated_global_listings,
     -- Total global value
     7000000 * AVG(estimated_property_value) AS estimated_total_value,
     -- Format as trillions for readability
     FORMAT("$%.2f trillion", 7000000 * AVG(estimated_property_value) / 1000000000000) AS total_value_trillions
   FROM 
     `airbnb-hackathon-project.airbnb_value.property_values`;
   ```

## Results
The analysis estimates the total value of real estate platformed by Airbnb globally to be approximately:

[PLACEHOLDER FOR FINAL RESULT] trillion USD

## Limitations and Future Improvements
- The analysis uses a simplified model that could be enhanced with more detailed data
- Regional variations in real estate markets could be incorporated
- Occupancy rates and cap rates could be adjusted based on location-specific factors
- Additional property characteristics (bedrooms, amenities, etc.) could be factored into the valuation



## Acknowledgments
- QVIDTVM for providing the datasets and challenge
- The Ion for hosting the Hack Day Competition