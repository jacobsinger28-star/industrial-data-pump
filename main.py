import os
import json
import csv
import io
import requests
from datetime import date, timedelta
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials

# =============================================================================
# CONFIGURATION
# =============================================================================
NAICS_ECOMM = "454110"
START_DATE   = "2022-01-01"   # baseline year start

# Markets: only NAICS-filterable sources (e-commerce exclusive)
# Population = 2023 Census estimates for the geographic area covered by each source
MARKETS = {
    "Houston":     {"type": "tx_permit", "tx_county": "061", "population": 4_780_913},
    "Dallas":      {"type": "tx_permit", "tx_county": "057", "population": 2_664_890},
    "Austin":      {"type": "tx_permit", "tx_county": "227", "population": 1_306_512},
    "San Antonio": {"type": "tx_permit", "tx_county": "029", "population": 2_103_026},
    "Fort Worth":  {"type": "tx_permit", "tx_county": "220", "population": 2_182_431},
    "Los Angeles": {"type": "la_license",                    "population": 3_898_747},
    "Seattle":     {"type": "seattle_license",               "population":   749_256},
}


# =============================================================================
# HELPERS
# =============================================================================
def fetch_paginated(url, page_size=1000):
    """Paginate through a Socrata endpoint and return all records."""
    all_records = []
    offset = 0
    while True:
        r = requests.get(f"{url}&$limit={page_size}&$offset={offset}")
        if r.status_code != 200:
            print(f"  API error {r.status_code}: {r.text[:200]}")
            break
        batch = r.json()
        all_records.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    return all_records


def get_last_n_complete_months(n):
    """Return list of YYYY-MM strings for the last n complete calendar months."""
    months = []
    d = date.today().replace(day=1)
    for _ in range(n):
        d = d - timedelta(days=1)        # last day of previous month
        months.append(d.strftime("%Y-%m"))
        d = d.replace(day=1)
    return months


# =============================================================================
# GOOGLE SHEETS SETUP
# =============================================================================
try:
    service_account_info = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_JSON'))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)
    gc = gspread.Client(auth=creds)
    spreadsheet = gc.open("Top 50 MSA Industrial Tracker")

    def get_or_create_sheet(name, rows=5000, cols=12):
        try:
            ws = spreadsheet.worksheet(name)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)
        return ws

    raw_sheet       = get_or_create_sheet("Raw_Registrations", rows=5000, cols=8)
    scorecard_sheet = get_or_create_sheet("Market_Scorecard",  rows=50,   cols=12)
    print("Connected to Google Sheets.")
except Exception as e:
    print(f"Error connecting to Google Sheets: {e}")
    exit()


# =============================================================================
# DATA COLLECTION
# =============================================================================
all_rows = []
market_monthly = defaultdict(lambda: defaultdict(int))  # market -> {YYYY-MM: count}

# --- TEXAS METROS (TX Comptroller Sales Tax Permits, NAICS 454110) ---
for city, cfg in {k: v for k, v in MARKETS.items() if v["type"] == "tx_permit"}.items():
    print(f"Fetching {city}, TX...")
    url = (
        "https://data.texas.gov/resource/jrea-zgmq.json"
        f"?outlet_naics_code={NAICS_ECOMM}"
        f"&taxpayer_county_code={cfg['tx_county']}"
        f"&$where=outlet_permit_issue_date>='{START_DATE}'"
        "&$order=outlet_permit_issue_date+DESC"
    )
    records = fetch_paginated(url)
    for item in records:
        d = item.get("outlet_permit_issue_date", "")
        month = d[:7] if len(d) >= 7 else None
        all_rows.append([
            d,
            item.get("taxpayer_name", "N/A"),
            city, "TX",
            "454110 - Electronic Shopping",
            "TX Sales Tax Permit"
        ])
        if month:
            market_monthly[city][month] += 1
    print(f"  {len(records)} records.")

# --- LOS ANGELES (LA City Office of Finance Business Licenses, NAICS 454110) ---
print("Fetching Los Angeles, CA...")
url = (
    "https://data.lacity.org/resource/6rrh-rzua.json"
    f"?naics={NAICS_ECOMM}"
    f"&$where=location_start_date>='{START_DATE}'"
    "&$order=location_start_date+DESC"
)
records = fetch_paginated(url)
for item in records:
    d = item.get("location_start_date", "")
    month = d[:7] if len(d) >= 7 else None
    all_rows.append([
        d,
        item.get("business_name", "N/A"),
        "Los Angeles", "CA",
        "454110 - Electronic Shopping",
        "LA City Business License"
    ])
    if month:
        market_monthly["Los Angeles"][month] += 1
print(f"  {len(records)} records.")

# --- SEATTLE (Seattle Business Licenses, NAICS 454111 = Electronic Shopping 2022) ---
# Note: Seattle uses 2022 NAICS vintage where 454110 was split into 454111/454112/454113
# Date format is YYYYMMDD (no dashes) — converted to YYYY-MM for scorecard
print("Fetching Seattle, WA...")
url = (
    "https://data.seattle.gov/resource/wnbq-64tb.json"
    "?naics_code=454111"
    f"&$where=license_start_date+>=+'{START_DATE.replace('-', '')}'"
    "&$order=license_start_date+DESC"
)
records = fetch_paginated(url)
for item in records:
    d = item.get("license_start_date", "")
    # Convert YYYYMMDD -> YYYY-MM-DD for consistency
    date_fmt = f"{d[:4]}-{d[4:6]}-{d[6:8]}" if len(d) == 8 else d
    month = f"{d[:4]}-{d[4:6]}" if len(d) == 8 else None
    all_rows.append([
        date_fmt,
        item.get("business_legal_name", "N/A"),
        "Seattle", "WA",
        "454111 - Electronic Shopping",
        "Seattle Business License"
    ])
    if month:
        market_monthly["Seattle"][month] += 1
print(f"  {len(records)} records.")

# Write raw data (headers + all records)
raw_sheet.append_row(["Date", "Business Name", "City", "State", "Industry", "Source"])
if all_rows:
    raw_sheet.append_rows(all_rows)
print(f"\nWrote {len(all_rows)} rows to Raw_Registrations.")


# =============================================================================
# SCORECARD COMPUTATION
# =============================================================================
last_3_months   = get_last_n_complete_months(3)
baseline_months = [f"2022-{str(m).zfill(2)}" for m in range(1, 13)]

scorecard_rows = []

for market, cfg in MARKETS.items():
    monthly    = market_monthly[market]
    state      = "TX" if cfg["type"] == "tx_permit" else "CA"
    population = cfg["population"]

    # Current rate: average of last 3 complete months
    recent_counts = [monthly.get(m, 0) for m in last_3_months]
    avg_recent    = sum(recent_counts) / len(recent_counts) if recent_counts else 0

    # Baseline: average of 2022 months that have data
    baseline_counts = [monthly[m] for m in baseline_months if m in monthly]
    avg_baseline    = sum(baseline_counts) / len(baseline_counts) if baseline_counts else 0

    # Per capita (per 100k population per month)
    per_100k = round((avg_recent / population) * 100_000, 2) if population else 0

    # Growth vs 2022 baseline
    if avg_baseline > 0:
        pct_vs_baseline = f"{round(((avg_recent - avg_baseline) / avg_baseline) * 100, 1)}%"
    else:
        pct_vs_baseline = "N/A (no 2022 data)"

    # Peak month
    peak = max(monthly.items(), key=lambda x: x[1]) if monthly else ("N/A", 0)

    # Total formations since START_DATE
    total = sum(monthly.values())

    scorecard_rows.append([
        market, state, f"{population:,}",
        round(avg_recent, 1),
        per_100k,
        round(avg_baseline, 1),
        pct_vs_baseline,
        peak[0], peak[1],
        total,
        "High — NAICS 454110 only"
    ])

# Rank by per-100k formation rate (best demand density first)
scorecard_rows.sort(key=lambda x: x[4], reverse=True)

scorecard_sheet.append_row([
    "Market", "State", "Population",
    "Avg Formations/mo (last 3mo)", "Per 100k/mo",
    "2022 Baseline avg/mo", "vs 2022 Baseline",
    "Peak Month", "Peak Month Count",
    "Total Since 2022",
    "Data Quality"
])
scorecard_sheet.append_rows(scorecard_rows)
print(f"Scorecard written for {len(scorecard_rows)} markets.")


# =============================================================================
# BFS NATIONAL TREND — Census Bureau Business Formation Statistics
# Source: https://www.census.gov/econ/bfs/csv/naics2.csv
# Weekly business applications for NAICS 44-45 (Retail Trade, includes e-commerce)
# National level only — serves as macro backdrop for market scorecard
# =============================================================================
print("\nFetching Census BFS national trend (NAICS 44-45)...")
try:
    bfs_sheet = get_or_create_sheet("BFS_National_Trend", rows=500, cols=5)

    r = requests.get("https://www.census.gov/econ/bfs/csv/naics2.csv")
    r.raise_for_status()

    reader = csv.DictReader(io.StringIO(r.text))
    retail_row = next((row for row in reader if row.get("naics2", "").strip() == "44-45"), None)

    if retail_row:
        # Extract all weekly columns from 2022 onward
        weekly_data = [
            (col, int(float(val)))
            for col, val in retail_row.items()
            if col.startswith("202") and "w" in col and val not in ("", "NA", None)
        ]
        weekly_data.sort(key=lambda x: x[0])  # sort chronologically

        # Compute 4-week moving average
        values = [v for _, v in weekly_data]
        bfs_rows = []
        for i, (week, apps) in enumerate(weekly_data):
            ma4 = round(sum(values[max(0, i-3):i+1]) / min(i+1, 4), 1)
            # Derive approximate date label from week code (e.g. 2024w03)
            year, wnum = week.split("w")
            bfs_rows.append([week, year, int(wnum), apps, ma4])

        bfs_sheet.append_row([
            "Week Code", "Year", "Week #",
            "NAICS 44-45 Applications (Retail incl. E-comm)",
            "4-Week Moving Avg"
        ])
        bfs_sheet.append_rows(bfs_rows)
        print(f"  {len(bfs_rows)} weekly data points written.")
    else:
        print("  Could not find NAICS 44-45 row in BFS CSV.")

except Exception as e:
    print(f"  BFS fetch error: {e}")

print(f"\nRun complete — {date.today()}")
