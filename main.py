import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- SETUP: Connect to Google Sheets ---
try:
    service_account_info = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_JSON'))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.Client(auth=creds)
    spreadsheet = gc.open("Top 50 MSA Industrial Tracker")
    try:
        sheet = spreadsheet.worksheet("Raw_Registrations")
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title="Raw_Registrations", rows=1000, cols=10)
    print("Connected to Google Sheets.")
except Exception as e:
    print(f"Error connecting to Google Sheets: {e}")
    exit()

all_rows = []

# =============================================================================
# SOURCE 1: TEXAS METROS — TX Comptroller Sales Tax Permits
# API: data.texas.gov/resource/jrea-zgmq.json
# NAICS filter: 454110 (Electronic Shopping / E-commerce)
# Fields: outlet_permit_issue_date, taxpayer_name, taxpayer_county_code
# =============================================================================
TX_MARKETS = {
    "Houston":     "061",  # Harris County
    "Dallas":      "057",  # Dallas County
    "Austin":      "227",  # Travis County
    "San Antonio": "029",  # Bexar County
    "Fort Worth":  "220",  # Tarrant County
}

for city, county_code in TX_MARKETS.items():
    print(f"Fetching {city}, TX...")
    url = (
        "https://data.texas.gov/resource/jrea-zgmq.json"
        "?outlet_naics_code=454110"
        f"&taxpayer_county_code={county_code}"
        "&$order=outlet_permit_issue_date+DESC&$limit=10"
    )
    r = requests.get(url)
    if r.status_code == 200:
        records = r.json()
        for item in records:
            all_rows.append([
                item.get('outlet_permit_issue_date', 'N/A'),
                item.get('taxpayer_name', 'N/A'),
                city, "TX",
                "454110 - Electronic Shopping",
                "TX Sales Tax Permit"
            ])
        print(f"  {len(records)} records.")
    else:
        print(f"  ERROR {r.status_code}: {r.text}")

# =============================================================================
# SOURCE 2: LOS ANGELES — LA City Office of Finance Business Licenses
# API: data.lacity.org/resource/6rrh-rzua.json
# NAICS filter: 454110 (Electronic Shopping / E-commerce)
# Fields: location_start_date, business_name, zip_code, naics
# =============================================================================
print("Fetching Los Angeles, CA...")
url = (
    "https://data.lacity.org/resource/6rrh-rzua.json"
    "?naics=454110"
    "&$order=location_start_date+DESC&$limit=20"
)
r = requests.get(url)
if r.status_code == 200:
    records = r.json()
    for item in records:
        all_rows.append([
            item.get('location_start_date', 'N/A'),
            item.get('business_name', 'N/A'),
            "Los Angeles", "CA",
            "454110 - Electronic Shopping",
            "LA City Business License"
        ])
    print(f"  {len(records)} records.")
else:
    print(f"  ERROR {r.status_code}: {r.text}")

# =============================================================================
# SOURCE 3: CHICAGO — City of Chicago Business Licenses
# API: data.cityofchicago.org/resource/uupf-x98q.json
# Note: No NAICS codes — uses city's own business_activity classification.
#       Filtered to recently issued licenses across all business types.
# Fields: date_issued, legal_name, zip_code, business_activity
# =============================================================================
print("Fetching Chicago, IL...")
url = (
    "https://data.cityofchicago.org/resource/uupf-x98q.json"
    "?$order=date_issued+DESC&$limit=20"
)
r = requests.get(url)
if r.status_code == 200:
    records = r.json()
    for item in records:
        all_rows.append([
            item.get('date_issued', 'N/A'),
            item.get('legal_name', 'N/A'),
            "Chicago", "IL",
            item.get('business_activity', 'N/A'),
            "Chicago Business License"
        ])
    print(f"  {len(records)} records.")
else:
    print(f"  ERROR {r.status_code}: {r.text}")

# =============================================================================
# SOURCE 4: DENVER — CO Secretary of State Entity Registrations
# API: data.colorado.gov/resource/p2ts-5ef2.json
# Note: No NAICS codes — covers all entity types filtered by Denver zip codes.
# Fields: entityformdate, entityname, entitytype, principalzipcode
# =============================================================================
DENVER_ZIPS = [
    "80202", "80203", "80204", "80205", "80206", "80207", "80209",
    "80210", "80211", "80212", "80214", "80218", "80219", "80220",
    "80222", "80223", "80224", "80226", "80227", "80230", "80231",
    "80237", "80246", "80247"
]

print("Fetching Denver, CO...")
denver_count = 0
for zip_code in DENVER_ZIPS:
    url = (
        "https://data.colorado.gov/resource/p2ts-5ef2.json"
        f"?principalzipcode={zip_code}"
        "&$order=entityformdate+DESC&$limit=5"
    )
    r = requests.get(url)
    if r.status_code == 200:
        for item in r.json():
            all_rows.append([
                item.get('entityformdate', 'N/A'),
                item.get('entityname', 'N/A'),
                "Denver", "CO",
                item.get('entitytype', 'N/A'),
                "CO SOS Entity Registration"
            ])
            denver_count += 1
    else:
        print(f"  ERROR {r.status_code} for zip {zip_code}")
print(f"  {denver_count} records.")

# =============================================================================
# SOURCE 5: CINCINNATI — City of Cincinnati Business Licenses
# API: data.cincinnati-oh.gov/resource/7dk3-gngs.json
# Note: No NAICS codes — uses internal revenue codes.
# Fields: entrydate, business_name, loczip
# =============================================================================
print("Fetching Cincinnati, OH...")
url = (
    "https://data.cincinnati-oh.gov/resource/7dk3-gngs.json"
    "?$order=entrydate+DESC&$limit=20"
)
r = requests.get(url)
if r.status_code == 200:
    records = r.json()
    for item in records:
        all_rows.append([
            item.get('entrydate', 'N/A'),
            item.get('business_name', 'N/A'),
            "Cincinnati", "OH",
            item.get('revenue_code', 'N/A'),
            "Cincinnati Business License"
        ])
    print(f"  {len(records)} records.")
else:
    print(f"  ERROR {r.status_code}: {r.text}")

# =============================================================================
# SOURCE 6: PHILADELPHIA — City of Philadelphia Business Licenses
# API: phl.carto.com (Carto SQL API)
# Note: No NAICS codes — uses licensetype strings.
# Fields: initialissuedate, business_name, zip, licensetype
# =============================================================================
print("Fetching Philadelphia, PA...")
url = (
    "https://phl.carto.com/api/v2/sql"
    "?q=SELECT+business_name,initialissuedate,zip,licensetype"
    "+FROM+li_business_licenses"
    "+WHERE+licensestatus='Active'"
    "+AND+licensetype+NOT+LIKE+'Rental%'"
    "+ORDER+BY+initialissuedate+DESC"
    "+LIMIT+20"
    "&format=json"
)
r = requests.get(url)
if r.status_code == 200:
    data = r.json()
    for item in data.get('rows', []):
        all_rows.append([
            item.get('initialissuedate', 'N/A'),
            item.get('business_name', 'N/A'),
            "Philadelphia", "PA",
            item.get('licensetype', 'N/A'),
            "Philadelphia Business License"
        ])
    print(f"  {len(data.get('rows', []))} records.")
else:
    print(f"  ERROR {r.status_code}: {r.text}")

# =============================================================================
# WRITE TO SHEET
# =============================================================================
if all_rows:
    sheet.append_rows(all_rows)
    print(f"\nDone! Added {len(all_rows)} rows to the sheet.")
else:
    print("No data found.")
