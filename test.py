import csv
import random
import time
import requests
from datetime import datetime
import pandas as pd

# Configuration
INPUT_CSV = 'addresses.csv'
OUTPUT_RESULTS_CSV = 'api_comparison_results.csv'
OUTPUT_SUMMARY_CSV = 'accuracy_summary.csv'
OUTPUT_LATENCY_CSV = 'latency_metrics.csv'
MAX_TEST_ROWS = 10  # Adjust based on free tier limits
RATE_LIMIT_OPENCAGE = 1  # 1 request per second
API_KEYS = {
    'geoapify': 'cce3c54e4ea34332b9995209823f58a4',
    'opencage': '0543d973b5a8442da7b5e3a7ebd7325f'
}

def load_and_prepare_data():
    """Load CSV data and prepare formatted addresses"""
    df = pd.read_csv(INPUT_CSV)
    df = df[df['countryCodeV2'].isin(['US', 'CA'])]
    # Create properly formatted address strings
    df['full_address'] = df['formatted'].apply(
        lambda x: ', '.join(eval(x)) if isinstance(x, str) else ''
    )
    
    # Filter and sample data
    valid_rows = df[df['full_address'].str.len() > 0]
    if len(valid_rows) > MAX_TEST_ROWS:
        valid_rows = valid_rows.sample(n=MAX_TEST_ROWS, random_state=42)
    
    return valid_rows[['full_address', 'provinceCode', 'countryCodeV2', 'zip']]

def call_geocoding_api(api_name, address):
    """Make API call to either Geoapify or OpenCage"""
    base_urls = {
        'geoapify': 'https://api.geoapify.com/v1/geocode/search',
        'opencage': 'https://api.opencagedata.com/geocode/v1/json'
    }
    
    params = {
        'geoapify': {'text': address, 'apiKey': API_KEYS['geoapify']},
        'opencage': {'q': address, 'key': API_KEYS['opencage']}
    }
    
    start_time = time.time()
    try:
        response = requests.get(base_urls[api_name], params=params[api_name])
        response.raise_for_status()
        data = response.json()
        latency = time.time() - start_time
        
        # Parse results
        if api_name == 'geoapify':
            if data['features']:
                props = data['features'][0]['properties']
                return {
                    'state': props.get('state_code'),
                    'country': props.get('country_code'),
                    'postcode': props.get('postcode'),
                    'latency': latency
                }
        elif api_name == 'opencage':
            if data['results']:
                components = data['results'][0]['components']
                return {
                    'state': components.get('state_code'),
                    'country': components.get('country_code'),
                    'postcode': components.get('postcode'),
                    'latency': latency
                }
        return None
    except Exception as e:
        print(f"Error with {api_name} API: {str(e)}")
        return None

def process_addresses(df):
    """Process addresses through both APIs with rate limiting"""
    results = []
    latency_data = []
    
    for idx, row in df.iterrows():
        # Get ground truth
        truth = {
            'state': row['provinceCode'],
            'country': row['countryCodeV2'],
            'postcode': str(row['zip']).strip()
        }
        
        # Process with Geoapify
        geoapify_start = datetime.now()
        geoapify_result = call_geocoding_api('geoapify', row['full_address'])
        geoapify_time = (datetime.now() - geoapify_start).total_seconds()
        
        # Process with OpenCage (with rate limiting)
        time.sleep(1 / RATE_LIMIT_OPENCAGE)  # Respect rate limit
        opencage_start = datetime.now()
        opencage_result = call_geocoding_api('opencage', row['full_address'])
        opencage_time = (datetime.now() - opencage_start).total_seconds()
        
        # Store results
        result_row = {
            'input_address': row['full_address'],
            'truth_state': truth['state'],
            'truth_country': truth['country'],
            'truth_postcode': truth['postcode'],
            'geoapify_state': geoapify_result.get('state') if geoapify_result else None,
            'geoapify_country': geoapify_result.get('country') if geoapify_result else None,
            'geoapify_postcode': geoapify_result.get('postcode') if geoapify_result else None,
            'opencage_state': opencage_result.get('state') if opencage_result else None,
            'opencage_country': opencage_result.get('country') if opencage_result else None,
            'opencage_postcode': opencage_result.get('postcode') if opencage_result else None
        }
        results.append(result_row)
        
        # Store latency data
        if geoapify_result and opencage_result:
            latency_data.append({
                'address': row['full_address'],
                'geoapify_latency': geoapify_result.get('latency'),
                'opencage_latency': opencage_result.get('latency')
            })
        
        print(f"Processed {idx+1}/{len(df)}: {row['full_address']}")
    
    return results, latency_data

def calculate_metrics(results_df):
    """Calculate accuracy metrics"""
    metrics = {
        'geoapify_correct_country': 0,
        'geoapify_correct_state': 0,
        'geoapify_correct_postcode': 0,
        'geoapify_correct_all': 0,
        'opencage_correct_country': 0,
        'opencage_correct_state': 0,
        'opencage_correct_postcode': 0,
        'opencage_correct_all': 0,
        'total_rows': len(results_df)
    }
    
    for _, row in results_df.iterrows():
        # Geoapify checks
        geo_country = str(row['geoapify_country']).lower() == str(row['truth_country']).lower()
        geo_state = str(row['geoapify_state']).lower() == str(row['truth_state']).lower()
        geo_postcode = str(row['geoapify_postcode']).replace(" ", "").lower() == str(row['truth_postcode']).replace(" ", "").lower()
        
        metrics['geoapify_correct_country'] += int(geo_country)
        metrics['geoapify_correct_state'] += int(geo_state)
        metrics['geoapify_correct_postcode'] += int(geo_postcode)
        metrics['geoapify_correct_all'] += int(geo_country and geo_state and geo_postcode)
        
        # OpenCage checks
        oc_country = str(row['opencage_country']).lower() == str(row['truth_country']).lower()
        oc_state = str(row['opencage_state']).lower() == str(row['truth_state']).lower()
        oc_postcode = str(row['opencage_postcode']).replace(" ", "").lower() == str(row['truth_postcode']).replace(" ", "").lower()
        
        metrics['opencage_correct_country'] += int(oc_country)
        metrics['opencage_correct_state'] += int(oc_state)
        metrics['opencage_correct_postcode'] += int(oc_postcode)
        metrics['opencage_correct_all'] += int(oc_country and oc_state and oc_postcode)
    
    # Convert counts to percentages
    for key in metrics:
        if key != 'total_rows':
            metrics[key] = (metrics[key] / metrics['total_rows']) * 100
    
    return metrics

def save_results(results, latency_data, metrics):
    """Save all results to CSV files"""
    # Save detailed results
    results_df = pd.DataFrame(results)
    results_df.to_csv(OUTPUT_RESULTS_CSV, index=False)
    
    # Save latency data
    latency_df = pd.DataFrame(latency_data)
    latency_stats = {
        'api': ['geoapify', 'opencage'],
        'avg_latency': [
            latency_df['geoapify_latency'].mean(),
            latency_df['opencage_latency'].mean()
        ],
        'min_latency': [
            latency_df['geoapify_latency'].min(),
            latency_df['opencage_latency'].min()
        ],
        'max_latency': [
            latency_df['geoapify_latency'].max(),
            latency_df['opencage_latency'].max()
        ]
    }
    pd.DataFrame(latency_stats).to_csv(OUTPUT_LATENCY_CSV, index=False)
    
    # Save accuracy metrics
    metrics_df = pd.DataFrame([metrics])
    metrics_df.to_csv(OUTPUT_SUMMARY_CSV, index=False)

def main():
    print("Loading and preparing data...")
    df = load_and_prepare_data()
    
    print(f"Processing {len(df)} addresses...")
    results, latency_data = process_addresses(df)
    
    print("Calculating metrics...")
    metrics = calculate_metrics(pd.DataFrame(results))
    
    print("Saving results...")
    save_results(results, latency_data, metrics)
    
    print("Comparison complete!")

if __name__ == '__main__':
    main()