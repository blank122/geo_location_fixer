# Import required libraries
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from tqdm import tqdm  # for progress bars
import time
import os

# ========== CONFIGURATION SECTION ==========
INPUT_CSV = "load41_city.csv"           # Input file with location data
OUTPUT_CSV = "tagged_geolocation_1.csv" # Output file with updated accuracy
CHECKPOINT_EVERY = 1000                 # Save progress every 1000 rows
MAX_THREADS = 5                         # Max threads for parallel geocoding (keep below 10 for Nominatim's policy)

# ========== INITIALIZATION ==========
geolocator = Nominatim(user_agent="geo_checker", timeout=10)  # Initialize the geocoder

# Load CSV and assign column names
df = pd.read_csv(INPUT_CSV, header=None, names=[
    "id", "city", "city1", "country", "latitude", "longitude", "state"
])

# Add a new column if not already present
if "geo_accuracy" not in df.columns:
    df["geo_accuracy"] = "unchecked"

# Filter only rows that haven’t been geocoded yet
df_to_process = df[df["geo_accuracy"] == "unchecked"]

# ========== UTILITY FUNCTIONS ==========

def normalize_name(name):
    """
    Normalizes city/country names for comparison: lowercase, no punctuation.
    """
    return str(name).strip().lower().replace(".", "").replace(",", "")

def check_location(index, lat, lon, city, country):
    """
    Uses reverse geocoding to verify if the coordinates match the expected city and country.
    Retries 3 times on failure.
    """
    retries = 3
    for attempt in range(retries):
        try:
            # Reverse geocode the coordinates
            location = geolocator.reverse((lat, lon), language='en')

            # If no location was found, mark as unknown
            if not location or "address" not in location.raw:
                print(f"[{index}] ❌ No address found for coordinates ({lat}, {lon})")
                return index, "unknown"
            
            # Extract address information
            address = location.raw["address"]
            rev_city = address.get("city") or address.get("town") or address.get("village") or address.get("municipality") or address.get("suburb") or ""
            rev_country = address.get("country", "")

            # Normalize values for comparison
            expected_city = normalize_name(city)
            expected_country = normalize_name(country)
            actual_city = normalize_name(rev_city)
            actual_country = normalize_name(rev_country)

            print(f"[{index}] 🔍 Expected: ({expected_city}, {expected_country}) | Actual: ({actual_city}, {actual_country})")

            # Compare the expected vs actual location
            if expected_city == actual_city and expected_country == actual_country:
                print(f"[{index}] ✅ Match confirmed for city: '{rev_city}', country: '{rev_country}'")
                return index, "accurate"
            else:
                print(f"[{index}] ❌ Mismatch.")
                return index, "inaccurate"
        except GeocoderTimedOut:
            print(f"[{index}] ⏱ Timeout on attempt {attempt + 1}")
            time.sleep(1)
        except Exception as e:
            print(f"[{index}] 🛑 ERROR: {e}")
            break
    return index, "unknown"

# ========== PROCESSING SECTION ==========

batch = []  # Collects rows to be processed in a batch

# Iterate over each unchecked row
for i, row in tqdm(df_to_process.iterrows(), total=len(df_to_process)):
    batch.append((i, row.latitude, row.longitude, str(row.city).lower(), str(row.country).lower()))

    # Process batch when full or at the last row
    if len(batch) >= CHECKPOINT_EVERY or i == df_to_process.index[-1]:
        print(f"\n🔄 Starting batch of {len(batch)} rows at index {i}...")

        results = []

        # Multithreaded processing using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            # Submit each location check to the thread pool
            futures = {executor.submit(check_location, *entry): entry[0] for entry in batch}

            for future in as_completed(futures, timeout=300):  # Wait max 5 minutes per batch
                index = futures[future]
                try:
                    result_index, result_status = future.result(timeout=30)  # Timeout per task
                    df.at[result_index, "geo_accuracy"] = result_status  # Update DataFrame
                except FutureTimeoutError:
                    print(f"[{index}] Task timed out.")
                    df.at[index, "geo_accuracy"] = "timeout"
                except Exception as e:
                    print(f"[{index}] Task failed: {e}")
                    df.at[index, "geo_accuracy"] = "error"

        # Save a checkpoint to CSV after each batch
        print(f"✅ Batch completed. Saving checkpoint...")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Checkpoint saved to {OUTPUT_CSV}\n")

        batch = []  # Clear batch for next cycle
