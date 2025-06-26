import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import os

# Parameters
INPUT_CSV = "load41_city.csv"
OUTPUT_CSV = "tagged_geolocations.csv"
CHECKPOINT_EVERY = 1000
MAX_THREADS = 5  # Keep it below 10 for Nominatim

# Initialize geolocator
geolocator = Nominatim(user_agent="geo_checker", timeout=10)

# Load data
df = pd.read_csv(INPUT_CSV, header=None, names=[
    "id", "city", "city1", "country", "latitude", "longitude", "state"
])
if "geo_accuracy" not in df.columns:
    df["geo_accuracy"] = "unchecked"

# Only check unchecked rows
df_to_process = df[df["geo_accuracy"] == "unchecked"]

# Thread-safe reverse geocode function
def check_location(index, lat, lon, city, country):
    retries = 3
    for attempt in range(retries):
        try:
            location = geolocator.reverse((lat, lon), language='en')
            if not location or "address" not in location.raw:
                print(f"[{index}] No address found.")
                return index, "unknown"
            address = location.raw["address"]
            rev_city = (address.get("city") or address.get("town") or address.get("village") or "").lower()
            rev_country = (address.get("country") or "").lower()

            if city in rev_city and country in rev_country:
                print(f"[{index}] Accurate match: {rev_city}, {rev_country}")
                return index, "accurate"
            else:
                print(f"[{index}] Inaccurate match: expected ({city}, {country}) vs found ({rev_city}, {rev_country})")
                return index, "inaccurate"
        except GeocoderTimedOut as e:
            print(f"[{index}] Timeout. Retrying... (Attempt {attempt+1})")
            time.sleep(1)
        except Exception as e:
            print(f"[{index}] ERROR: {str(e)}")
            break
    return index, "unknown"


# Process in batches
batch = []
for i, row in tqdm(df_to_process.iterrows(), total=len(df_to_process)):
    batch.append((i, row.latitude, row.longitude, str(row.city).lower(), str(row.country).lower()))

    if len(batch) >= CHECKPOINT_EVERY or i == df_to_process.index[-1]:
        print(f"\n🔄 Starting batch of {len(batch)} rows at index {i}...")

        results = []
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = [executor.submit(check_location, *entry) for entry in batch]
            for future in as_completed(futures):
                index, result = future.result()
                df.at[index, "geo_accuracy"] = result

        print(f"✅ Batch completed. Saving checkpoint...")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Checkpoint saved to {OUTPUT_CSV}\n")
        batch = []
