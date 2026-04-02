import os, requests, datetime, sys

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
JOB_TRACKER_SHEET_ID = os.environ['JOB_TRACKER_SHEET_ID']
WC_SHEET_ID = os.environ['WC_SHEET_ID']

JOBBER_API = "https://api.getjobber.com/api/graphql"

print("Refreshing Jobber access token...")
token_resp = requests.post(
    "https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
          "grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN})

print(f"Token refresh HTTP status: {token_resp.status_code}")
token_data = token_resp.json()
if "access_token" not in token_data:
    print(f"Token refresh failed: {token_data}")
    sys.exit(1)

ACCESS_TOKEN = token_data["access_token"]
print("Access token refreshed ✓")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "X-JOBBER-GRAPHQL-VERSION": "2025-04-16"
}

def jobber_query(query):
    r = requests.post(JOBBER_API, headers=HEADERS, json={"query": query})
    if r.status_code != 200:
        print(f"Jobber API error: {r.status_code} {r.text[:300]}")
        return {}
    data = r.json()
    if "errors" in data:
        print(f"GraphQL errors: {data['errors']}")
    return data.get("data", {})

def sheets_put(sheet_id, range_name, values):
    encoded = requests.utils.quote(range_name, safe='')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded}?valueInputOption=USER_ENTERED&key={GOOGLE_API_KEY}"
    r = requests.put(url, json={"values": values, "range": range_name, "majorDimension": "ROWS"})
    return r.status_code

def sheets_clear(sheet_id, range_name):
    encoded = requests.utils.quote(range_name, safe='')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded}:clear?key={GOOGLE_API_KEY}"
    requests.post(url)

def sheets_get(sheet_id, range_name):
    encoded = requests.utils.quote(range_name, safe='')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded}?key={GOOGLE_API_KEY}"
    r = requests.get(url)
    return r.json().get("values", [])

today = datetime.date.today()
week_start = today - datetime.timedelta(days=(today.weekday() + 1) % 7)
week_end = week_start + datetime.timedelta(days=6)

# Step 2: Fetch jobs - first let's discover the schema
print("Fetching jobs...")
jobs_data = jobber_query("""
{
  jobs(filter: { status: [ACTIVE] }, first: 50) {
    nodes {
      id jobNumber title
      client { name }
      total
      lineItems(first: 50) {
        nodes { name unitPrice quantity }
      }
    }
    totalCount
  }
}
""")
jobs = jobs_data.get("jobs", {}).get("nodes", [])
print(f"Found {len(jobs)} jobs")

# Step 3: Fetch invoices with correct fields
print("Fetching invoices...")
invoices_data = jobber_query("""
{
  invoices(first: 100) {
    nodes {
      id invoiceNumber subject total
      invoiceStatus
      client { name }
      amounts { depositAmount outstanding }
      paymentsTotal
    }
  }
}
""")
invoices = invoices_data.get("invoices", {}).get("nodes", [])
print(f"Found {len(invoices)} invoices")

# Step 4: Fetch quotes
print("Fetching quotes...")
quotes_data = jobber_query("""
{
  quotes(first: 50) {
    nodes {
      id quoteNumber
      quoteStatus
      amounts { subtotal }
      createdAt
      client { name }
    }
  }
}
""")
quotes = quotes_data.get("quotes", {}).get("nodes", [])
print(f"Found {len(quotes)} quotes")

# Step 5: Build Job Tracker rows
print("Building Job Tracker data...")
job_rows = []
for job in jobs:
    total_price = float(job.get("total") or 0)
    
    # Match invoice to job by job number
    job_num = str(job.get("jobNumber",""))
    job_inv = None
    for inv in invoices:
        # Try to match by subject containing job number
        subj = inv.get("subject","") or ""
        if job_num in subj:
            job_inv = inv
            break
    
    invoice_status = job_inv.get("invoiceStatus","N/A") if job_inv else "N/A"
    invoice_total = float(job_inv.get("total") or total_price) if job_inv else total_price
    outstanding = float(job_inv.get("amounts",{}).get("outstanding") or 0) if job_inv else 0
    collected = float(job_inv.get("paymentsTotal") or 0) if job_inv else 0

    job_rows.append([
        job.get("jobNumber",""),
        job.get("client",{}).get("name",""),
        job.get("title",""),
        "Active",
        f"${invoice_total:.2f}",
        "$0.00",  # Sub costs - expenses API needs separate call
        "$0.00",  # Mat costs
        "$0.00",  # Other costs
        "$0.00",  # Total costs
        f"${invoice_total:.2f}",  # Net (no costs yet)
        invoice_status,
        f"${outstanding:.2f}",
        f"${collected:.2f}",
        str(today)
    ])

if job_rows:
    sheets_clear(JOB_TRACKER_SHEET_ID, "Job Tracker!A5:N54")
    status = sheets_put(JOB_TRACKER_SHEET_ID, "Job Tracker!A5:N54", job_rows)
    print(f"Job Tracker updated: HTTP {status} ({len(job_rows)} jobs)")
else:
    print("No jobs to write")

# Step 6: Weekly collections from invoices
weekly_collections = 0
for inv in invoices:
    paid = float(inv.get("paymentsTotal") or 0)
    # We'll count all paid invoices for now since we don't have payment dates
    if inv.get("invoiceStatus") == "PAID":
        weekly_collections += paid

# Quotes converted this week
weekly_new_sales = 0
for q in quotes:
    if q.get("quoteStatus") == "CONVERTED":
        amt = q.get("amounts",{})
        weekly_new_sales += float(amt.get("subtotal") or 0)

print(f"Week collections: ${weekly_collections:.2f}, new sales: ${weekly_new_sales:.2f}")

# Step 7: Write to WC KBPI
kbpi_rows = sheets_get(WC_SHEET_ID, "Key Business Performance Indicators!A3:I60")
week_end_str = f"{week_end.month}/{week_end.day}"
target_row = None
for i, row in enumerate(kbpi_rows):
    if len(row) > 1 and row[1].strip() == week_end_str:
        target_row = i + 3
        break

if target_row:
    s1 = sheets_put(WC_SHEET_ID, f"Key Business Performance Indicators!C{target_row}", [[weekly_new_sales]])
    s2 = sheets_put(WC_SHEET_ID, f"Key Business Performance Indicators!D{target_row}", [[weekly_collections]])
    print(f"KBPI row {target_row} updated: HTTP {s1}, {s2}")
else:
    print(f"Week ending {week_end_str} not found in KBPI tab")

print("Jobber sync complete!")
