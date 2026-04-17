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
naics2_rows = []  # cache for reuse in National_Historical tab
try:
    bfs_sheet = get_or_create_sheet("BFS_National_Trend", rows=500, cols=5)

    r = requests.get("https://www.census.gov/econ/bfs/csv/naics2.csv")
    r.raise_for_status()
    naics2_rows = list(csv.DictReader(io.StringIO(r.text)))

    retail_row = next((row for row in naics2_rows if row.get("naics2", "").strip() == "44-45"), None)

    if retail_row:
        weekly_data = [
            (col, int(float(val)))
            for col, val in retail_row.items()
            if col.startswith("202") and "w" in col and val not in ("", "NA", None)
        ]
        weekly_data.sort(key=lambda x: x[0])
        values = [v for _, v in weekly_data]
        bfs_rows = []
        for i, (week, apps) in enumerate(weekly_data):
            ma4 = round(sum(values[max(0, i-3):i+1]) / min(i+1, 4), 1)
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

# =============================================================================
# STATE FORMATION TRENDS — Census BFS State-Level Weekly Data
# Source: https://www.census.gov/econ/bfs/csv/bfs_state_apps_weekly_nsa.csv
# All industries, all 50 states + DC, weekly since 2006
# Tab shows annual totals + recent 4-week avg per state (one row per state)
# =============================================================================
print("\nFetching Census BFS state-level formation data...")
try:
    state_sheet = get_or_create_sheet("State_Formation_Trends", rows=60, cols=30)

    # --- Fetch 2023 state populations from Census PEP API ---
    # Maps full state name -> population (no API key required)
    STATE_NAME_TO_ABBR = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
        "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
        "District of Columbia": "DC", "Florida": "FL", "Georgia": "GA", "Hawaii": "HI",
        "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
        "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME",
        "Maryland": "MD", "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
        "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
        "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM",
        "New York": "NY", "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH",
        "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
        "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX",
        "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
        "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
        "Puerto Rico": "PR",
    }

    pop_by_abbr = {}
    pop_r = requests.get(
        "https://api.census.gov/data/2023/pep/charv?get=NAME,POP&YEAR=2023&for=state:*"
    )
    if pop_r.status_code == 200:
        pop_data = pop_r.json()
        headers = pop_data[0]
        name_idx = headers.index("NAME")
        pop_idx  = headers.index("POP")
        for row in pop_data[1:]:
            name = row[name_idx]
            abbr = STATE_NAME_TO_ABBR.get(name)
            if abbr:
                pop_by_abbr[abbr] = int(row[pop_idx])
        print(f"  Loaded population for {len(pop_by_abbr)} states.")
    else:
        print(f"  Population API error {pop_r.status_code} — per-capita will be N/A.")

    # --- Parse BFS state weekly CSV ---
    state_data    = defaultdict(lambda: defaultdict(list))
    early_wk_data = defaultdict(lambda: defaultdict(int))  # state -> year -> sum of weeks 1-12
    recent_weeks  = []

    bfs_r = requests.get("https://www.census.gov/econ/bfs/csv/bfs_state_apps_weekly_nsa.csv")
    bfs_r.raise_for_status()

    reader = csv.DictReader(io.StringIO(bfs_r.text))
    all_rows_state = list(reader)

    max_year = max(int(row["Year"]) for row in all_rows_state)
    max_week = max(int(row["Week"]) for row in all_rows_state if int(row["Year"]) == max_year)

    for row in all_rows_state:
        year  = int(row["Year"])
        week  = int(row["Week"])
        state = row["State"].strip()
        ba    = row["BA_NSA"].strip()
        hba   = row["HBA_NSA"].strip()
        if not ba or ba in (".", "NA"):
            continue
        ba_val = int(float(ba))
        state_data[state][year].append(ba_val)
        if week <= 12:
            early_wk_data[state][year] += ba_val
        if year == max_year and week > max_week - 4:
            recent_weeks.append((
                state, ba_val,
                int(float(hba)) if hba not in ("", ".", "NA") else 0
            ))

    recent_ba  = defaultdict(list)
    recent_hba = defaultdict(list)
    for state, ba, hba in recent_weeks:
        recent_ba[state].append(ba)
        recent_hba[state].append(hba)

    def per_100k(value, pop):
        if isinstance(value, int) and pop:
            return round((value / pop) * 100_000, 1)
        return "N/A"

    # Detect all years present in the data (dynamic — no hardcoding)
    all_years = sorted({int(row["Year"]) for row in all_rows_state})

    # --- Build summary rows ---
    summary_rows = []
    for state in sorted(state_data.keys()):
        yearly = state_data[state]
        pop = pop_by_abbr.get(state)

        def annual_total(yr):
            return sum(yearly.get(yr, [])) if yearly.get(yr) else "N/A"

        year_totals = {yr: annual_total(yr) for yr in all_years}

        # YoY growth: fixed 2024 vs 2025
        t2024 = year_totals.get(2024, "N/A")
        t2025 = year_totals.get(2025, "N/A")
        if isinstance(t2024, int) and isinstance(t2025, int) and t2024 > 0:
            yoy = f"{round(((t2025 - t2024) / t2024) * 100, 1)}%"
        else:
            yoy = "N/A"

        # Weeks 1-12 comparison: 2025 vs 2026
        w12_2025 = early_wk_data[state].get(2025) or "N/A"
        w12_2026 = early_wk_data[state].get(2026) or "N/A"
        if isinstance(w12_2025, int) and isinstance(w12_2026, int) and w12_2025 > 0:
            w12_chg = f"{round(((w12_2026 - w12_2025) / w12_2025) * 100, 1)}%"
        else:
            w12_chg = "N/A"

        r4_ba  = round(sum(recent_ba[state])  / len(recent_ba[state]),  1) if recent_ba[state]  else "N/A"
        r4_hba = round(sum(recent_hba[state]) / len(recent_hba[state]), 1) if recent_hba[state] else "N/A"

        row = [state, f"{pop:,}" if pop else "N/A"]
        for yr in all_years:
            t = year_totals[yr]
            row += [t, per_100k(t, pop)]
        row += [
            yoy,
            w12_2025, per_100k(w12_2025, pop),
            w12_2026, per_100k(w12_2026, pop),
            w12_chg,
            r4_ba,  per_100k(round(r4_ba  * 52) if isinstance(r4_ba,  float) else "N/A", pop),
            r4_hba, per_100k(round(r4_hba * 52) if isinstance(r4_hba, float) else "N/A", pop),
            f"Week {max_week}, {max_year}"
        ]
        summary_rows.append(row)

    # Sort by most recent full year per-100k descending
    most_recent_per100k_idx = 2 + (len(all_years) - 1) * 2 + 1  # index of last year's per-100k col
    summary_rows.sort(
        key=lambda x: x[most_recent_per100k_idx] if isinstance(x[most_recent_per100k_idx], float) else 0,
        reverse=True
    )

    header = ["State", "Population (2023)"]
    for yr in all_years:
        header += [f"{yr} Total Apps", f"{yr} Per 100k"]
    header += [
        "YoY Growth (2024→2025)",
        "Wks 1-12 2025 Apps", "Wks 1-12 2025 Per 100k",
        "Wks 1-12 2026 Apps", "Wks 1-12 2026 Per 100k",
        "Wks 1-12 Change (2025→2026)",
        "Recent 4-Wk Avg (Apps)", "Annualised Per 100k",
        "Recent 4-Wk Avg (High-Propensity)", "Annualised HPA Per 100k",
        "Data Through"
    ]

    state_sheet.append_row(header)
    state_sheet.append_rows(summary_rows)
    print(f"  State formation trends written for {len(summary_rows)} states ({min(all_years)}–{max_year}).")

except Exception as e:
    print(f"  State trends error: {e}")

# =============================================================================
# NATIONAL HISTORICAL — Annual business application totals since 2006
#
# Total BA + HBA: summed from bfs_state_apps_weekly_nsa.csv across all states
#   (already loaded above — no extra API call needed)
# Retail BA (NAICS 44-45): summed from naics2.csv (closest BFS proxy for e-comm)
#   NOTE: No NAICS 454 (nonstore retail) available in BFS national files.
#         NAICS 44-45 includes ALL retail — physical stores + e-commerce combined.
# =============================================================================
print("\nBuilding National_Historical tab...")
try:
    hist_sheet = get_or_create_sheet("National_Historical", rows=30, cols=10)

    # --- Total BA + HBA by year: sum all states from state CSV ---
    nat_ba  = defaultdict(int)   # year -> total BA
    nat_hba = defaultdict(int)   # year -> total HBA

    for row in all_rows_state:
        yr  = int(row["Year"])
        ba  = row["BA_NSA"].strip()
        hba = row["HBA_NSA"].strip()
        if ba  and ba  not in (".", "NA"):
            nat_ba[yr]  += int(float(ba))
        if hba and hba not in (".", "NA"):
            nat_hba[yr] += int(float(hba))

    # --- Retail BA (NAICS 44-45) by year: sum all weeks from naics2.csv ---
    nat_retail_ba = defaultdict(int)  # year -> retail BA

    if naics2_rows:
        retail_row = next(
            (row for row in naics2_rows if row.get("naics2", "").strip() == "44-45"),
            None
        )
        if retail_row:
            for col, val in retail_row.items():
                if "w" in col and val not in ("", "NA", None):
                    try:
                        yr = int(col.split("w")[0])
                        nat_retail_ba[yr] += int(float(val))
                    except (ValueError, IndexError):
                        pass

    # --- Build annual summary rows ---
    all_hist_years = sorted(set(nat_ba.keys()) | set(nat_hba.keys()))

    hist_rows = []
    for i, yr in enumerate(all_hist_years):
        ba       = nat_ba.get(yr, "N/A")
        hba      = nat_hba.get(yr, "N/A")
        retail   = nat_retail_ba.get(yr) or "N/A"

        # HBA share of total BA
        hba_pct = (
            f"{round((hba / ba) * 100, 1)}%"
            if isinstance(ba, int) and isinstance(hba, int) and ba > 0
            else "N/A"
        )
        # Retail share of total BA
        retail_pct = (
            f"{round((retail / ba) * 100, 1)}%"
            if isinstance(ba, int) and isinstance(retail, int) and ba > 0
            else "N/A"
        )
        # YoY growth (BA)
        if i > 0:
            prev_ba = nat_ba.get(all_hist_years[i - 1])
            yoy_ba = (
                f"{round(((ba - prev_ba) / prev_ba) * 100, 1)}%"
                if isinstance(ba, int) and isinstance(prev_ba, int) and prev_ba > 0
                else "N/A"
            )
        else:
            yoy_ba = "—"

        hist_rows.append([
            yr, ba, hba, hba_pct, retail, retail_pct, yoy_ba
        ])

    hist_sheet.append_row([
        "Year",
        "Total Business Applications (BA)",
        "High-Propensity Applications (HBA)",
        "HBA as % of Total",
        "Retail Trade Apps (NAICS 44-45)*",
        "Retail as % of Total",
        "YoY Growth (Total BA)",
    ])
    hist_sheet.append_rows(hist_rows)
    hist_sheet.append_row([
        "*NAICS 44-45 = all Retail Trade (physical + e-commerce). No e-commerce-only "
        "filter exists in Census BFS national data — NAICS 454 is not published separately."
    ])
    print(f"  National historical written for {len(hist_rows)} years "
          f"({min(all_hist_years)}–{max(all_hist_years)}).")

except Exception as e:
    print(f"  National historical error: {e}")

# =============================================================================
# METRO FORMATION TRENDS — Census BFS County Annual Data aggregated to MSAs
# Source: https://www.census.gov/econ/bfs/xlsx/bfs_county_apps_annual.xlsx
# Annual data 2005–2024 (2025 county data released ~mid-2026 by Census)
# Counties aggregated to MSAs; ranked by 2024 total applications
# =============================================================================
print("\nBuilding Metro_Formation_Trends tab...")
try:
    import pandas as pd
    import io as _io

    metro_sheet = get_or_create_sheet("Metro_Formation_Trends", rows=60, cols=35)

    # MSA definitions: MSA name -> list of 5-digit county FIPS codes (state+county)
    METRO_COUNTIES = {
        "New York–Newark":      ["36061","36047","36081","36005","36085","36059","36103",
                                  "36119","34003","34017","34031","34013","34039"],
        "Los Angeles–Long Beach":["06037","06059","06065","06071","06111"],
        "Chicago–Naperville":   ["17031","17043","17097","17197","17089","17111"],
        "Dallas–Fort Worth":    ["48113","48439","48085","48121","48139","48231","48397"],
        "Houston–Woodlands":    ["48201","48157","48339","48039","48167","48071","48291"],
        "Washington DC":        ["11001","24031","24033","51059","51013","51107","51153",
                                  "51510","51600","51610","24017","24021"],
        "Miami–Fort Lauderdale":["12086","12011","12099"],
        "Philadelphia":         ["42101","42091","42045","42017","42029","34005","34007","34015"],
        "Atlanta":              ["13121","13089","13135","13067","13063","13247","13151",
                                  "13077","13097","13113","13117","13143","13149","13159","13171"],
        "Phoenix–Mesa":         ["04013","04021"],
        "Boston":               ["25025","25017","25009","25021","25023","33015","33017"],
        "Riverside–San Bern.":  ["06065","06071"],
        "Seattle–Tacoma":       ["53033","53061","53053"],
        "Minneapolis–St. Paul": ["27053","27123","27037","27003","27019","27139","27163",
                                  "55093","55109"],
        "San Diego":            ["06073"],
        "Tampa–St. Pete":       ["12057","12103","12101","12105"],
        "Denver–Aurora":        ["08031","08005","08059","08001","08035","08014","08047"],
        "St. Louis":            ["29189","29510","29183","17163","17117","17119","17133"],
        "Baltimore":            ["24005","24510","24003","24013","24025","24027","51013"],
        "Orlando–Kissimmee":    ["12095","12097","12117","12069"],
        "Charlotte":            ["37119","37179","37025","37097","37057"],
        "San Antonio":          ["48029","48091","48187","48259","48325","48493"],
        "Portland–Vancouver":   ["41051","41067","41005","53011"],
        "Sacramento":           ["06067","06017","06061","06113"],
        "Pittsburgh":           ["42003","42005","42007","42019","42125","42129"],
        "Austin–Round Rock":    ["48453","48491","48209","48055","48021"],
        "Las Vegas":            ["32003"],
        "Cincinnati":           ["39061","39165","39025","39017","21037","21077","21117"],
        "Kansas City":          ["29095","29165","29037","29047","29107","20091","20209"],
        "Columbus OH":          ["39049","39041","39045","39089","39129","39127"],
        "Indianapolis":         ["18097","18059","18011","18063","18081","18145","18057"],
        "Cleveland":            ["39035","39093","39055","39085","39103"],
        "Nashville":            ["47037","47187","47149","47189","47021","47111","47141",
                                  "47015","47043","47165","47147"],
        "San Jose":             ["06085"],
        "San Francisco–Oakland":["06075","06081","06001","06013","06041"],
        "Jacksonville FL":      ["12031","12109","12019","12003","13069"],
        "Memphis":              ["47157","05069","28033","28137"],
        "Richmond VA":          ["51087","51041","51145","51760"],
        "Oklahoma City":        ["40109","40017","40027","40083","40125","40051"],
        "Raleigh–Durham":       ["37183","37063","37101","37135","37069"],
        "Hartford CT":          ["09003","09013"],
        "Birmingham AL":        ["01073","01009","01115","01121","01125","01117"],
        "New Orleans":          ["22071","22051","22089","22093","22095","22103"],
        "Buffalo NY":           ["36029","36063"],
        "Salt Lake City":       ["49035","49011","49057","49045"],
        "Louisville":           ["21111","21029","21163","21211","18019","18043"],
    }

    # Fetch county data
    bfs_county_r = requests.get(
        "https://www.census.gov/econ/bfs/xlsx/bfs_county_apps_annual.xlsx", timeout=30
    )
    bfs_county_r.raise_for_status()
    df_county = pd.read_excel(
        _io.BytesIO(bfs_county_r.content),
        sheet_name="County Data",
        header=1,
        skiprows=1
    )

    # Normalize county code to 5-digit string
    df_county["fips5"] = df_county["County Code"].astype(str).str.zfill(5)

    # BA year columns
    ba_cols = [c for c in df_county.columns if str(c).startswith("BA")]
    ba_years = [int(c[2:]) for c in ba_cols]   # e.g. [2005, 2006, ..., 2024]

    # Convert to numeric, coerce suppressed/missing to NaN
    for col in ba_cols:
        df_county[col] = pd.to_numeric(df_county[col], errors="coerce")

    # Build metro aggregates
    metro_rows = []
    for msa, fips_list in METRO_COUNTIES.items():
        sub = df_county[df_county["fips5"].isin(fips_list)]
        county_count = len(sub)
        if sub.empty:
            continue
        yearly_totals = {}
        for col, yr in zip(ba_cols, ba_years):
            s = sub[col].sum()
            yearly_totals[yr] = int(s) if not pd.isna(s) else "N/A"

        t2022 = yearly_totals.get(2022, "N/A")
        t2023 = yearly_totals.get(2023, "N/A")
        t2024 = yearly_totals.get(2024, "N/A")

        # 2023 vs 2022
        if isinstance(t2023, int) and isinstance(t2022, int) and t2022 > 0:
            yoy_23 = f"{round(((t2023 - t2022) / t2022) * 100, 1)}%"
        else:
            yoy_23 = "N/A"
        # 2024 vs 2023
        if isinstance(t2024, int) and isinstance(t2023, int) and t2023 > 0:
            yoy_24 = f"{round(((t2024 - t2023) / t2023) * 100, 1)}%"
        else:
            yoy_24 = "N/A"

        row = [msa, county_count]
        for yr in ba_years:
            row.append(yearly_totals.get(yr, "N/A"))
        row += [yoy_23, yoy_24, t2024, "2024"]
        metro_rows.append(row)

    # Sort by 2024 total (index = 2 + len(ba_years) - 1 = last BA col index)
    last_ba_idx = 1 + len(ba_years)  # 0=MSA, 1=counties, then ba years
    metro_rows.sort(key=lambda x: x[last_ba_idx] if isinstance(x[last_ba_idx], int) else 0, reverse=True)

    header_metro = ["Metro (MSA)", "# Counties"] + [str(yr) for yr in ba_years]
    header_metro += ["YoY 2022→2023", "YoY 2023→2024", "2024 Total", "Data Through"]

    metro_sheet.append_row(["*Annual BFS county data — 2025 county file expected ~mid-2026. "
                             "State-level 2025/2026 data available in State_Formation_Trends tab."])
    metro_sheet.append_row(header_metro)
    metro_sheet.append_rows(metro_rows)
    print(f"  Metro formation trends written for {len(metro_rows)} MSAs ({min(ba_years)}–{max(ba_years)}).")

except Exception as e:
    print(f"  Metro trends error: {e}")


print(f"\nRun complete — {date.today()}")
