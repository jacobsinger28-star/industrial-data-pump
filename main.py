import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# 1. SETUP: Connect to your "Digital Safe" (GitHub Secrets)
try:
    # This pulls the long JSON password you pasted into GitHub Secrets
    service_account_info = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_JSON'))
    
    # These are the permissions the robot needs
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Authenticate with Google
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.authorize(creds)
    
    # Open your specific Google Sheet
    # MAKE SURE the sheet name matches exactly what you named it
    spreadsheet = gc.open("Top 50 MSA Industrial Tracker")
    sheet = spreadsheet.worksheet("Raw_Registrations")
    
    print("Successfully connected to Google Sheets.")

except Exception as e:
    print(f"Error connecting to Google Sheets: {e}")
    exit()

# 2. THE DATA PUMPS: Fetching from TN and TX
all_rows_to_add = []

# --- MARKET 1: NASHVILLE (TN BERO) ---
# Filtering for E-commerce (NAICS 454110) in Davidson County
print("Fetching Nashville data...")
tn_url = "https://data.tn.gov/resource/dagr-u2hb.json?naics_code=454110&county=DAVIDSON"
tn_response = requests.get(tn_url)
if tn_response.status_code == 200:
    for item in tn_response.json()[:10]: # Grab the latest 10
        all_rows_to_add.append([
            item.get('registration_date', 'N/A'),
            item.get('business_name', 'N/A'),
            "Nashville",
            "E-commerce (454110)"
        ])

# --- MARKET 2: HOUSTON/TOMBALL (TX Sales Tax Permits) ---
# Filtering for Harris County (Houston)
print("Fetching Houston data...")
tx_url = "https://data.texas.gov/resource/jrea-zgmq.json?taxpayer_county=HARRIS"
tx_response = requests.get(tx_url)
if tx_response.status_code == 200:
    for item in tx_response.json()[:10]:
        all_rows_to_add.append([
            item.get('permit_issue_date', 'N/A'),
            item.get('taxpayer_name', 'N/A'),
            "Houston",
            "Sales Tax Permit"
        ])

# 3. DELIVERY: Writing to your Sheet
if all_rows_to_add:
    # This adds all the collected rows to the bottom of your sheet at once
    sheet.append_rows(all_rows_to_add)
    print(f"Success! Added {len(all_rows_to_add)} new business leads to your sheet.")
else:
    print("No new data found this time.")
