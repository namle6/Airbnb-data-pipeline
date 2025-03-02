import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io

# Define pipeline options
options = PipelineOptions([
    '--project=airbnb-valuation-project',
    '--runner=DataflowRunner',
    '--region=us-central1',
    '--temp_location=gs://airbnb-dataflow-temp/temp',
    '--staging_location=gs://airbnb-dataflow-temp/staging',
    '--job_name=airbnb-data-processing'
])

# Define parsing function - handles different CSV formats
def parse_csv(line, filename=None):
    reader = csv.reader(io.StringIO(line))
    row = next(reader)
    
    # Basic record with defaults
    record = {
        'id': 'unknown',
        'name': '',
        'price': 0.0,
        'room_type': 'unknown',
        'property_type': 'regular',
        'region': 'unknown',
        'dataset_source': filename or 'unknown'
    }
    
    # Try to extract data based on common fields
    try:
        # Look for price fields (checking different positions and formats)
        if len(row) > 9 and row[9] and row[9].replace('.', '').isdigit():
            record['price'] = float(row[9])
        elif len(row) > 2 and row[2] and row[2].replace(',', '').replace('.', '').isdigit():
            record['price'] = float(row[2].replace(',', ''))
        
        # Extract ID when available
        if row[0] and row[0] != 'id':
            record['id'] = row[0]
        else:
            record['id'] = f"generated-{hash(str(row))}"
        
        # Extract name/location when available
        if len(row) > 1:
            record['name'] = row[1]
            
        # Try to determine property type from name or other fields
        lower_name = record['name'].lower()
        if 'luxury' in lower_name or record['price'] > 5000:
            record['property_type'] = 'luxury'
        elif 'desert' in lower_name or 'sahara' in lower_name:
            record['property_type'] = 'desert'
            
    except Exception as e:
        print(f"Error parsing row: {e}")
        
    return record

def estimate_property_value(record):
    # Default values
    price = float(record.get('price', 0))
    property_type = record.get('property_type', 'regular')
    
    # Determine cap rate based on property type
    if property_type == 'luxury':
        cap_rate = 0.035  # 3.5% for luxury
    elif property_type == 'desert':
        cap_rate = 0.045  # 4.5% for desert
    else:
        cap_rate = 0.05   # 5% for regular
    
    # Estimate occupancy based on property type
    if property_type == 'luxury':
        occupancy = 0.60  # 60% for luxury
    elif property_type == 'desert':
        occupancy = 0.55  # 55% for desert/seasonal
    else:
        occupancy = 0.65  # 65% for regular
    
    # Calculate annual revenue and property value
    annual_revenue = price * 365 * occupancy
    property_value = annual_revenue / cap_rate if cap_rate > 0 else 0
    
    # Add calculated fields to record
    record['estimated_occupancy'] = occupancy
    record['cap_rate'] = cap_rate
    record['estimated_annual_revenue'] = annual_revenue
    record['estimated_property_value'] = property_value
    
    return record

# Define the pipeline
def run():
    with beam.Pipeline(options=options) as p:
        # Read and process all CSV files
        processed_data = (
            p
            | "Read CSV Files" >> beam.io.ReadFromText('gs://airbnb-raw-data/*.csv', skip_header_lines=1)
            | "Parse CSV" >> beam.Map(parse_csv)
            | "Filter Valid Records" >> beam.Filter(lambda r: float(r.get('price', 0)) > 0)
            | "Estimate Property Values" >> beam.Map(estimate_property_value)
        )
        
        # Write the results to BigQuery
        processed_data | "Write to BigQuery" >> beam.io.WriteToBigQuery(
            'airbnb_valuation.listings',
            schema='id:STRING,name:STRING,price:FLOAT,room_type:STRING,property_type:STRING,region:STRING,dataset_source:STRING,estimated_occupancy:FLOAT,cap_rate:FLOAT,estimated_annual_revenue:FLOAT,estimated_property_value:FLOAT',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    run()