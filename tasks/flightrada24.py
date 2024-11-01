import pandas as pd
import flightradar24
import os

# Initialize the Flightradar24 API
fr_api = flightradar24.Api()

# Define the output directory
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "../data/flightradar24")

def save_to_csv(data, base_file_name):
    # Ensure the output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Split data into chunks of 1000
    for i in range(0, len(df), 1000):
        chunk = df.iloc[i:i + 1000]  # Get a chunk of 1000 records
        file_path = os.path.join(OUTPUT_DIR, f"{base_file_name}_{i // 1000 + 1}.csv")  # Create a unique file name
        chunk.to_csv(file_path, index=False)
        print(f"Saved {len(chunk)} records to {file_path}")

def fetch_data():
    # Fetch and save airports data
    airports = fr_api.get_airports()
    save_to_csv(airports, 'airports')

    # Fetch and save airlines data
    airlines = fr_api.get_airlines()
    save_to_csv(airlines, 'airlines')

def main():
    fetch_data()

if __name__ == "__main__":
    main()
