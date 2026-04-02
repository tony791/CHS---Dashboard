import os, requests, datetime, sys, json

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
JOB_TRACKER_SHEET_ID = os.environ['JOB_TRACKER_SHEET_ID']
WC_SHEET_ID = os.environ['WC_SHEET_ID']

JOB_TRACKER_SCRIPT = "https://script.google.com/macros/s/AKfycbwSatQFSlvP8GY0XXMRjgdN8esc5IJo4T4cTj1kwBShjJ_iRLOc3fnGZ-ezfGbNx5nU/exec"
WC_TRACKER_SCRIPT  = "https://script.google.com/macros/s/AKfycbzN2yakVRoBYdSa-F-UreDLzl8YctxvCt1vCAqPJEk8kGSIKD8ak-qkSRJwpjNCd_Gm/exec"

JOBBER_API = "https://api.getjobber.com/api/graphql"

# Step 1: Refresh access token
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

def script_get(script_url, payload):
    try:
        r = requests.get(script_url,
            params={"payload": json.dumps(payload)},
            timeout=30)
        return r.text[:200]
    except Exception as ex:
        return f"Error: {ex}"

def script_write_rows(script_url, tab, start_row, values, clear_end_row=54):
    # First clear the range
    clear_payload = {
        "tab": tab,
        "range": f"A{start_row}:R{clear_end_row}",
        "values": [],
        "clear": True,
        "clearRange": f"A{start_row}:R{clear_end_row}"
    }
    resp = script_get(script_url, clear_payload)
    print(f"Clear response: {resp}")

    # Write each row individually
    success = 0
    for i, row in enumerate(values):
        row_num = start_row + i
        payload = {
            "tab": tab,
            "range": f"A{row_num}:R{row_num}",
            "values": [row]
        }
        resp = script_get(script_url, payload)
        if '"ok"' in resp:
            success += 1
        elif i < 2 or i == len(values)-1:
            print(f"Row {row_num}: {resp}")
    print(f"Wrote {success}/{len(values)} rows successfully")

def sheets_get(sheet_id, range_name):
    encoded = requests.utils.quote(range_name, safe='')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded}?key={GOOGLE_API_KEY}"
    r = requests.get(url)
    return r.json().get("values", [])

today = datetime.date.today()
week_start = today - datetime.timedelta(days=(today.weekday() + 1) % 7)
week_end = week_start + datetime.timedelta(days=6)

# Step 2: Fetch jobs with property address and contact info
print("Fetching jobs...")
jobs_data = jobber_query("""
{
  jobs(first: 50) {
    nodes {
      id jobNumber title createdAt
      client { 
        name 
        phones { number }
        emails { address }
      }
      property {
        address {
          street
          city
          province
        }
      }
      total
    }
    totalCount
  }
}
""")
jobs_all = jobs_data.get("jobs", {}).get("nodes", [])
jobs = [j for j in jobs_all if (j.get("createdAt") or "").startswith("2026")]
print(f"Found {len(jobs_all)} total jobs, {len(jobs)} from 2026")

# Step 3: Fetch invoices
print("Fetching invoices...")
invoices_data = jobber_query("""
{
  invoices(first: 100) {
    nodes {
      id invoiceNumber subject total
      invoiceStatus
      client { name }
      amounts { depositAmount }
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

# Step 5: Build Job Tracker rows matching exact column layout:
# A: Job#  B: Customer Name  C: Address  D: Phone  E: Email
# F: Sub Cost  G: Materials  H: Other Expenses  I: Total Costs
# J: Markup(x0.70)  K: Job Total  L: Status  M: Quote Given
# N: Quote Approved  O: Pymt Scheduled  P: Pymt Collected
# Q: Lead Source  R: Notes

print("Building Job Tracker data...")
job_rows = []
for job in jobs:
    total_price = float(job.get("total") or 0)
    job_num = str(job.get("jobNumber",""))

    # Get address
    prop = job.get("property") or {}
    addr = prop.get("address") or {}
    street = addr.get("street","")
    city = addr.get("city","")
    address = f"{street}, {city}".strip(", ") if street or city else ""

    # Get phone and email
    phones = job.get("client",{}).get("phones",[]) or []
    emails = job.get("client",{}).get("emails",[]) or []
    phone = phones[0].get("number","") if phones else ""
    email = emails[0].get("address","") if emails else ""

    # Match invoice by job number in subject
    job_inv = None
    for inv in invoices:
        subj = inv.get("subject","") or ""
        if job_num in subj:
            job_inv = inv
            break

    invoice_status = job_inv.get("invoiceStatus","N/A") if job_inv else "N/A"
    invoice_total = float(job_inv.get("total") or total_price) if job_inv else total_price
    payments_total = float(job_inv.get("paymentsTotal") or 0) if job_inv else 0
    deposit = float((job_inv.get("amounts") or {}).get("depositAmount") or 0) if job_inv else 0

    # Costs (will be $0 until expenses API is added)
    sub_cost = 0
    mat_cost = 0
    other_cost = 0
    total_cost = sub_cost + mat_cost + other_cost
    markup = invoice_total * 0.70
    net = invoice_total - total_cost

    job_rows.append([
        job_num,           # A: Job #
        job.get("client",{}).get("name",""),  # B: Customer Name
        address,           # C: Address
        phone,             # D: Phone
        email,             # E: Email
        f"${sub_cost:.2f}",   # F: Sub Cost
        f"${mat_cost:.2f}",   # G: Materials
        f"${other_cost:.2f}", # H: Other Expenses
        f"${total_cost:.2f}", # I: Total Costs
        f"${markup:.2f}",     # J: Markup
        f"${invoice_total:.2f}", # K: Job Total
        invoice_status,    # L: Status
        "",                # M: Quote Given
        "",                # N: Quote Approved
        f"${deposit:.2f}", # O: Pymt Scheduled
        f"${payments_total:.2f}", # P: Pymt Collected
        "Jobber",          # Q: Lead Source
        ""                 # R: Notes
    ])

if job_rows:
    print(f"Writing {len(job_rows)} jobs to Job Tracker...")
    script_write_rows(JOB_TRACKER_SCRIPT, "Job Tracker", 5, job_rows)
else:
    print("No jobs to write")

# Step 6: Weekly metrics
weekly_collections = 0
for inv in invoices:
    if inv.get("invoiceStatus") == "PAID":
        weekly_collections += float(inv.get("paymentsTotal") or 0)

weekly_new_sales = 0
for q in quotes:
    if q.get("quoteStatus") == "CONVERTED":
        weekly_new_sales += float(q.get("amounts",{}).get("subtotal") or 0)

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
    payload = {
        "tab": "Key Business Performance Indicators",
        "range": f"C{target_row}:D{target_row}",
        "values": [[weekly_new_sales, weekly_collections]]
    }
    resp = script_get(WC_TRACKER_SCRIPT, payload)
    print(f"KBPI row {target_row} updated: {resp}")
else:
    print(f"Week ending {week_end_str} not found in KBPI tab")

print("Jobber sync complete!")
