import os
import json
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from datetime import datetime

def compress_json_files():
    # Get all date directories in snapshots
    snapshots_dir = Path('snapshots')
    date_dirs = [d for d in snapshots_dir.iterdir() if d.is_dir() and not d.name.startswith('.')]
    
    # List to store all records
    all_records = []
    
    # Process each date directory
    for date_dir in tqdm(date_dirs, desc="Processing directories"):
        # Get all JSON files in the directory
        json_files = list(date_dir.glob('*.json'))
        
        # Process each JSON file
        for json_file in tqdm(json_files, desc=f"Processing {date_dir.name}", leave=False):
            timestamp = int(json_file.stem)  # Get filename without extension
            try:
                with open(json_file, 'r') as f:
                    data = json.load(f)
                
                # Convert timestamp to datetime for better querying
                dt = datetime.fromtimestamp(timestamp)
                
                # Flatten the data structure
                if isinstance(data, list):
                    for station in data:
                        station['timestamp'] = dt
                        all_records.append(station)
                else:
                    data['timestamp'] = dt
                    all_records.append(data)
                    
            except json.JSONDecodeError:
                print(f"Error reading {json_file}")
                continue
    
    # Convert to DataFrame
    df = pd.DataFrame(all_records)
    
    # Create output directory if it doesn't exist
    output_dir = Path('analysis/compressed')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save as Parquet with compression
    output_file = output_dir / 'all_data.parquet'
    df.to_parquet(
        output_file,
        compression='snappy',  # Fast compression algorithm
        index=False
    )
    
    print(f"\nData saved to {output_file}")
    print(f"Number of records: {len(df)}")
    print("\nDataFrame Info:")
    print(df.info())

if __name__ == "__main__":
    compress_json_files() 