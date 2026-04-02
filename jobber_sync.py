import os, json, requests, datetime

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
JOB_TRACKER_SHEET_ID = os.environ['JOB_TRACKER_SHEET_ID']
WC_SHEET_ID = os.environ['WC_SHEET_ID']

JOBBER_API = "https://api.getjobber.com/api/graphql"

# Step 1: Refresh access token
print("Refreshing Jobber access token...")
token_resp = requests.post("https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/json"},
    json={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    })

token_data = token_resp.json()
if "access_token" not in token_data:
    print(f"Token refresh failed: {token_data}")
    exit(1)

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
    return r.json().get("data", {})

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

# Step 2: Fetch active jobs
print("Fetching active jobs...")
jobs_data = jobber_query("""
{
  jobs(filter: { status: [ACTIVE] }, first: 50) {
    nodes {
      id jobNumber title
      client { name }
      total createdAt
      expenses(first: 50) {
        nodes { title total category }
      }
    }
    totalCount
  }
}
""")
jobs = jobs_data.get("jobs", {}).get("nodes", [])
print(f"Found {len(jobs)} active jobs")

# Step 3: Fetch invoices
print("Fetching invoices...")
invoices_data = jobber_query("""
{
  invoices(first: 100) {
    nodes {
      id invoiceNumber subject total amountOwing status
      issuedDate dueDate
      client { name }
      job { jobNumber title }
      payments { nodes { amount receivedAt } }
    }
  }
}
""")
invoices = invoices_data.get("invoices", {}).get("nodes", [])
print(f"Found {len(invoices)} invoices")

# Step 4: Fetch converted quotes
print("Fetching converted quotes...")
quotes_data = jobber_query("""
{
  quotes(filter: { status: [CONVERTED] }, first: 50) {
    nodes {
      id quoteNumber total convertedAt
      client { name }
    }
  }
}
""")
quotes = quotes_data.get("quotes", {}).get("nodes", [])
print(f"Found {len(quotes)} converted quotes")

# Step 5: Build Job Tracker rows
print("Building Job Tracker data...")
job_rows = []
for job in jobs:
    expenses = job.get("expenses", {}).get("nodes", [])
    sub_cost = sum(float(e.get("total") or 0) for e in expenses if e.get("category") == "SUBCONTRACTOR")
    mat_cost = sum(float(e.get("total") or 0) for e in expenses if e.get("category") == "MATERIALS")
    other_cost = sum(float(e.get("total") or 0) for e in expenses if e.get("category") not in ["SUBCONTRACTOR","MATERIALS"])
    total_cost = sub_cost + mat_cost + other_cost
    total_price = float(job.get("total") or 0)
    net_profit = total_price - total_cost

    job_num = str(job.get("jobNumber",""))
    job_invoices = [i for i in invoices if i.get("job") and str(i["job"].get("jobNumber","")) == job_num]
    inv = job_invoices[0] if job_invoices else {}
    invoice_status = inv.get("status","N/A")
    amount_owing = float(inv.get("amountOwing") or 0)
    invoice_total = float(inv.get("total") or total_price)

    collected = sum(float(p.get("amount") or 0) for p in inv.get("payments",{}).get("nodes",[]) if inv)

    job_rows.append([
        job.get("jobNumber",""),
        job.get("client",{}).get("name",""),
        job.get("title",""),
        "Active",
        f"${invoice_total:.2f}",
        f"${sub_cost:.2f}",
        f"${mat_cost:.2f}",
        f"${other_cost:.2f}",
        f"${total_cost:.2f}",
        f"${net_profit:.2f}",
        invoice_status,
        f"${amount_owing:.2f}",
        f"${collected:.2f}",
        str(today)
    ])

if job_rows:
    sheets_clear(JOB_TRACKER_SHEET_ID, "Job Tracker!A5:N54")
    status = sheets_put(JOB_TRACKER_SHEET_ID, "Job Tracker!A5:N54", job_rows)
    print(f"Job Tracker updated: HTTP {status} ({len(job_rows)} jobs)")
else:
    print("No active jobs to write")

# Step 6: Weekly collections and new sales
print(f"Calculating week {week_start} to {week_end}...")

weekly_collections = 0
for inv in invoices:
    for pmt in inv.get("payments",{}).get("nodes",[]):
        paid_str = (pmt.get("receivedAt") or "")[:10]
        if paid_str:
            try:
                paid_date = datetime.date.fromisoformat(paid_str)
                if week_start <= paid_date <= week_end:
                    weekly_collections += float(pmt.get("amount") or 0)
            except: pass

weekly_new_sales = 0
for q in quotes:
    conv_str = (q.get("convertedAt") or "")[:10]
    if conv_str:
        try:
            conv_date = datetime.date.fromisoformat(conv_str)
            if week_start <= conv_date <= week_end:
                weekly_new_sales += float(q.get("total") or 0)
        except: pass

print(f"Week collections: ${weekly_collections:.2f}")
print(f"Week new sales: ${weekly_new_sales:.2f}")

# Step 7: Write to WC KBPI tab
kbpi_rows = sheets_get(WC_SHEET_ID, "Key Business Performance Indicators!A3:I60")
week_end_str = f"{week_end.month}/{week_end.day}"
print(f"Looking for week ending {week_end_str} in KBPI tab...")

target_row = None
for i, row in enumerate(kbpi_rows):
    if len(row) > 1 and row[1].strip() == week_end_str:
        target_row = i + 3
        break

if target_row:
    s1 = sheets_put(WC_SHEET_ID, f"Key Business Performance Indicators!C{target_row}", [[weekly_new_sales]])
    s2 = sheets_put(WC_SHEET_ID, f"Key Business Performance Indicators!D{target_row}", [[weekly_collections]])
    print(f"KBPI row {target_row} updated: New Sales HTTP {s1}, Collections HTTP {s2}")
else:
    print(f"Warning: Week ending {week_end_str} not found in KBPI tab")

print("✓ Jobber sync complete!")
