# Import required libraries
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from tqdm import tqdm  # for progress bars
import time
import os
from fuzzywuzzy import fuzz  # Install with: pip install fuzzywuzzy python-Levenshtein

# ========== CONFIGURATION SECTION ==========
INPUT_CSV = "load41_city.csv"           # Input file with location data
OUTPUT_CSV = "tagged_geolocation_1.csv" # Output file with updated accuracy
# In your configuration section:
CHECKPOINT_EVERY = 500  # Reduce batch size from 1000 to 500
MAX_THREADS = 2         # Reduce from 5 to 2 or even 1 to comply with Nominatim's policy
TIMEOUT_PER_BATCH = 600 # Increase from 300 to 600 seconds (10 minutes)
REQUEST_DELAY = 1.1  # 1.1 seconds between requests (slightly more than Nominatim's 1/sec limit)

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
# Add this near the top of your script
STATE_PROVINCE_MAPPING = {
    # US States
    'US': {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    },
    # Canadian Provinces/Territories
    'CA': {
        'AB': 'Alberta',
        'BC': 'British Columbia',
        'MB': 'Manitoba',
        'NB': 'New Brunswick',
        'NL': 'Newfoundland and Labrador',
        'NT': 'Northwest Territories',
        'NS': 'Nova Scotia',
        'NU': 'Nunavut',
        'ON': 'Ontario',
        'PE': 'Prince Edward Island',
        'QC': 'Quebec',
        'SK': 'Saskatchewan',
        'YT': 'Yukon'
    }
}

# Create reverse mappings
REVERSE_MAPPINGS = {
    country_code: {v.lower(): k for k, v in mappings.items()}
    for country_code, mappings in STATE_PROVINCE_MAPPING.items()
}


def normalize_name(name, country_code=None):
    """More comprehensive normalization with country-specific handling"""
    if not isinstance(name, str) or not name.strip():
        return ""
    
    name = str(name).strip().lower()
    
    # Handle state/province abbreviations
    if country_code and country_code.upper() in STATE_PROVINCE_MAPPING:
        mapping = STATE_PROVINCE_MAPPING[country_code.upper()]
        if name.upper() in mapping:
            return mapping[name.upper()].lower()
    
    replacements = {
        ".": "", 
        ",": "",
        "town of ": "",
        "city of ": "",
        "united states": "us",
        "usa": "us",
        "canada": "ca",
        "  ": " "  # double space to single
    }
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name

def check_location(index, lat, lon, city, country, state_abbrev):
    retries = 3
    backoff_factor = 1.5  # Time between retries will be (backoff_factor)^attempt

    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY * (attempt + 1))  # Increasing delay with each retry
            
            location = geolocator.reverse((lat, lon), exactly_one=True, addressdetails=True)
            
            if not location or not hasattr(location, 'raw') or not location.raw.get('address'):
                return index, "unknown"
            
            address = location.raw["address"]
            
            # Extract possible city names
            city_fields = [
                'neighbourhood', 'suburb', 'hamlet',
                'village', 'town', 'city', 
                'municipality', 'county'
            ]
            rev_city = next(
                (address[field] for field in city_fields 
                 if field in address and address[field]), 
                ""
            )
            
            rev_country = normalize_name(address.get('country', ''))
            rev_state = normalize_name(address.get('state', ''))
            
            # Normalize all values with country context
            expected_city = normalize_name(city)
            expected_country = normalize_name(country)
            expected_state = normalize_name(state_abbrev, country)
            actual_city = normalize_name(rev_city)
            
            print(f"[{index}] 🔍 Expected: ({expected_city}, {state_abbrev}, {expected_country}) | "
                  f"Actual: ({actual_city}, {rev_state}, {rev_country})")

            # Country comparison
            country_match = (
                expected_country == rev_country or
                fuzz.ratio(expected_country, rev_country) > 85
            )
            
            if not country_match:
                print(f"[{index}] ❌ Country mismatch")
                return index, "inaccurate_country"
            
            # State/province comparison
            state_match = False
            if expected_state and rev_state:
                # Get possible reverse mappings
                rev_state_abbrev = REVERSE_MAPPINGS.get(country.upper(), {}).get(rev_state, '')
                
                state_match = (
                    expected_state == rev_state or  # Full name match
                    state_abbrev.lower() == rev_state_abbrev.lower() or  # Abbrev match
                    fuzz.ratio(expected_state, rev_state) > 70  # Fuzzy match
                )
            
            # City matching with fuzzy logic
            city_match = False
            if actual_city:
                city_match = (
                    expected_city in actual_city or 
                    actual_city in expected_city or
                    fuzz.ratio(expected_city, actual_city) > 70
                )
            
            # Decision logic
            if city_match and state_match:
                print(f"[{index}] ✅ Full match")
                return index, "accurate"
            elif state_match and not actual_city:
                print(f"[{index}] ⚠ State/province match (no city)")
                return index, "state_only_match"
            elif state_match:
                print(f"[{index}] ⚠ State/province matches but city doesn't")
                return index, "state_match_city_mismatch"
            else:
                print(f"[{index}] ❌ Complete mismatch")
                return index, "inaccurate"
                
        except GeocoderTimedOut:
            wait_time = backoff_factor ** (attempt + 1)
            print(f"[{index}] ⏱ Timeout on attempt {attempt + 1}, waiting {wait_time:.1f}s")
            time.sleep(wait_time)
            if attempt == retries - 1:
                return index, "timeout"
        except Exception as e:
            print(f"[{index}] 🛑 ERROR: {str(e)}")
            if attempt == retries - 1:
                return index, "error"
    return index, "unknown"

# ========== PROCESSING SECTION ==========

batch = []  # Collects rows to be processed in a batch

# Iterate over each unchecked row
for i, row in tqdm(df_to_process.iterrows(), total=len(df_to_process)):
    # Now passing state as an additional parameter
    batch.append((i, row.latitude, row.longitude, str(row.city), str(row.country), str(row.state)))

    # Process batch when full or at the last row
    if len(batch) >= CHECKPOINT_EVERY or i == df_to_process.index[-1]:
        print(f"\n🔄 Starting batch of {len(batch)} rows at index {i}...")

        # Multithreaded processing using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
            futures = {executor.submit(check_location, *entry): entry[0] for entry in batch}
            
            try:
                for future in as_completed(futures, timeout=TIMEOUT_PER_BATCH):
                    index = futures[future]
                    try:
                        result_index, result_status = future.result(timeout=30)
                        df.at[result_index, "geo_accuracy"] = result_status
                    except FutureTimeoutError:
                        print(f"[{index}] Task timed out.")
                        df.at[index, "geo_accuracy"] = "timeout"
                    except Exception as e:
                        print(f"[{index}] Task failed: {e}")
                        df.at[index, "geo_accuracy"] = "error"
            except TimeoutError:
                print(f"⚠️ Batch timed out with {len(futures)} unfinished tasks. Saving progress...")
                # Mark unfinished tasks as "timeout"
                for future in futures:
                    if not future.done():
                        df.at[futures[future], "geo_accuracy"] = "timeout"

        # Save a checkpoint to CSV after each batch
        print(f"✅ Batch completed. Saving checkpoint...")
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"💾 Checkpoint saved to {OUTPUT_CSV}\n")

        batch = []  # Clear batch for next cycle