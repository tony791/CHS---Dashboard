import os, requests, datetime, sys, json, time

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
JOB_TRACKER_SHEET_ID = os.environ['JOB_TRACKER_SHEET_ID']
WC_SHEET_ID = os.environ['WC_SHEET_ID']

JOB_TRACKER_SCRIPT = "https://script.google.com/macros/s/AKfycbwSatQFSlvP8GY0XXMRjgdN8esc5IJo4T4cTj1kwBShjJ_iRLOc3fnGZ-ezfGbNx5nU/exec"
WC_TRACKER_SCRIPT  = "https://script.google.com/macros/s/AKfycbzN2yakVRoBYdSa-F-UreDLzl8YctxvCt1vCAqPJEk8kGSIKD8ak-qkSRJwpjNCd_Gm/exec"

JOBBER_API = "https://api.getjobber.com/api/graphql"

# ── Token refresh ─────────────────────────────────────────────
print("Refreshing Jobber access token...")
token_resp = requests.post(
    "https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
          "grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN})
print(f"Token refresh HTTP: {token_resp.status_code}")
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
        print(f"Jobber API error: {r.status_code} {r.text[:200]}")
        return {}
    data = r.json()
    if "errors" in data:
        print(f"GraphQL errors: {data['errors']}")
    return data.get("data", {})

def script_get(script_url, payload):
    try:
        r = requests.get(script_url,
            params={"payload": json.dumps(payload)},
            timeout=60)
        return r.text[:300]
    except Exception as ex:
        return f"Error: {ex}"

def script_write_rows(script_url, tab, start_row, values, clear_end_row=60):
    clear_payload = {
        "tab": tab, "clear": True,
        "clearRange": f"A{start_row}:R{clear_end_row}",
        "range": f"A{start_row}:R{clear_end_row}",
        "values": [["","","","","","","","","","","","","","","","","",""]]
    }
    resp = script_get(script_url, clear_payload)
    print(f"Clear: {resp[:60]}")
    success = 0
    for i, row in enumerate(values):
        row_num = start_row + i
        payload = {"tab": tab, "range": f"A{row_num}:R{row_num}", "values": [row]}
        resp = script_get(script_url, payload)
        if '"ok"' in resp:
            success += 1
        else:
            print(f"Row {row_num} error: {resp[:80]}")
    print(f"Wrote {success}/{len(values)} rows to {tab}")

def sheets_get(sheet_id, range_name):
    encoded = requests.utils.quote(range_name, safe='')
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{encoded}?key={GOOGLE_API_KEY}"
    r = requests.get(url)
    return r.json().get("values", [])

def fmt_date(dt_str):
    if not dt_str: return ""
    try: return dt_str[:10]
    except: return ""

def fmt_money(val):
    return f"${float(val or 0):.2f}"

today = datetime.date.today()
week_start = today - datetime.timedelta(days=(today.weekday() + 1) % 7)
week_end = week_start + datetime.timedelta(days=6)

# ── Fetch jobs with all needed data ───────────────────────────
print("Fetching jobs...")
jobs_data = jobber_query("""
{
  jobs(first: 50) {
    nodes {
      id jobNumber title createdAt
      jobStatus
      startAt completedAt
      source
      client {
        name
        phones { number }
        emails { address }
        customFields {
          ... on CustomFieldText { label valueText }
          ... on CustomFieldDropdown { label valueDropdown }
        }
      }
      property {
        address { street city province postalCode }
      }
      total
      quote {
        quoteNumber quoteStatus
        createdAt
        transitionedAt
        amounts { subtotal }
      }
      lineItems(first: 20) {
        nodes { name quantity unitPrice unitCost }
      }
      visits(first: 1) {
        nodes { id startAt }
        totalCount
      }
      paymentRecords(first: 10) {
        nodes { amount }
      }
      invoices(first: 1) {
        nodes {
          id total invoiceStatus paymentsTotal
          issuedDate dueDate
          amounts { depositAmount }
        }
      }
    }
    totalCount
  }
}
""")
jobs_all = jobs_data.get("jobs", {}).get("nodes", [])
jobs = [j for j in jobs_all if (j.get("createdAt") or "").startswith("2026")]
print(f"Found {len(jobs_all)} total jobs, {len(jobs)} from 2026")

# ── Fetch expenses per 2026 job ────────────────────────────────
expenses_by_job = {}
print("Fetching expenses per job...")
for job in jobs:
    job_id = job.get("id","")
    job_num = str(job.get("jobNumber",""))
    exp_data = jobber_query("""
{
  job(id: "%s") {
    expenses(first: 50) {
      nodes { title description total }
    }
  }
}
""" % job_id)
    expenses = exp_data.get("job",{}).get("expenses",{}).get("nodes",[]) or []
    expenses_by_job[job_num] = expenses
    if expenses:
        total_exp = sum(float(e.get("total") or 0) for e in expenses)
        print(f"  Job #{job_num}: ${total_exp:.2f} expenses")
    time.sleep(0.3)
print(f"Fetched expenses for {len(expenses_by_job)} jobs")

# ── Fetch all invoices for weekly collections ──────────────────
print("Fetching invoices...")
invoices_data = jobber_query("""
{
  invoices(first: 100) {
    nodes {
      id total invoiceStatus paymentsTotal
      issuedDate dueDate
      paymentRecords(first: 10) {
        nodes { amount }
      }
    }
  }
}
""")
invoices = invoices_data.get("invoices", {}).get("nodes", [])
print(f"Found {len(invoices)} invoices")

# ── Build Job Tracker rows ─────────────────────────────────────
print("Building Job Tracker data...")
job_rows = []
ytd_revenue = 0.0
ytd_cost    = 0.0

for job in jobs:
    job_num = str(job.get("jobNumber",""))
    total_price = float(job.get("total") or 0)
    job_status_raw = (job.get("jobStatus") or "").lower()
    client_name = job.get("client",{}).get("name","")

    # Address
    prop = job.get("property") or {}
    addr = prop.get("address") or {}
    street = addr.get("street","") or ""
    city = addr.get("city","") or ""
    address = f"{street}, {city}".strip(", ")

    # Contact
    phones = job.get("client",{}).get("phones",[]) or []
    emails_list = job.get("client",{}).get("emails",[]) or []
    phone = phones[0].get("number","") if phones else ""
    email = emails_list[0].get("address","") if emails_list else ""

    # Lead source
    custom_fields = job.get("client",{}).get("customFields",[]) or []
    referred_by = ""
    for cf in custom_fields:
        if cf.get("label","").lower() in ["referred by","referral","lead source"]:
            referred_by = cf.get("valueText","") or cf.get("valueDropdown","") or ""
            break
    source_map = {
        "QUOTE_CONVERT": "Quote",
        "GQL_API": "Jobber",
        "WEB_APP": "Web App",
        "CLIENT_HUB": "Client Hub",
        "MANUAL": "Manual",
    }
    raw_source = str(job.get("source") or "")
    lead_source = referred_by or source_map.get(raw_source, raw_source or "Jobber")

    # Costs
    line_items = job.get("lineItems",{}).get("nodes",[]) or []
    sub_cost = sum(float(li.get("unitCost") or 0) * float(li.get("quantity") or 1) for li in line_items)
    expenses = expenses_by_job.get(job_num, [])
    mat_cost = sum(float(e.get("total") or 0) for e in expenses)
    other_cost = 0.0
    total_cost = sub_cost + mat_cost + other_cost
    net_profit = total_price - total_cost

    # Invoice data
    job_invoices = job.get("invoices",{}).get("nodes",[]) or []
    inv = job_invoices[0] if job_invoices else {}
    inv_status = (inv.get("invoiceStatus","") or "").upper()
    payments_total = float(inv.get("paymentsTotal") or 0)
    deposit_amt = float((inv.get("amounts") or {}).get("depositAmount") or 0)

    payment_records = job.get("paymentRecords",{}).get("nodes",[]) or []
    num_payments = len(payment_records)
    deposit_date = ""
    final_payment_date = ""
    if deposit_amt > 0 and num_payments >= 1:
        deposit_date = fmt_date(inv.get("issuedDate",""))
    if inv_status == "PAID":
        final_payment_date = fmt_date(inv.get("dueDate",""))

    quote = job.get("quote") or {}
    quote_sent = fmt_date(quote.get("createdAt",""))
    quote_status = (quote.get("quoteStatus","") or "").lower()
    quote_approved = fmt_date(quote.get("transitionedAt","")) if quote_status in ["approved","converted"] else ""

    has_visits = (job.get("visits",{}).get("totalCount") or 0) > 0
    if inv_status == "PAID":
        status = "Completed"
    elif inv_status in ["SENT","VIEWED","PAST_DUE"]:
        status = "Awaiting Payment"
    elif job_status_raw in ["requires_invoicing"]:
        status = "Awaiting Payment"
    elif job_status_raw in ["completed","archived"]:
        status = "Completed"
    elif has_visits:
        status = "In Progress"
    else:
        status = "Need to Schedule"

    # Accumulate YTD totals (all 2026 jobs)
    ytd_revenue += total_price
    ytd_cost    += total_cost

    job_rows.append([
        job_num, client_name, address, phone, email,
        fmt_money(sub_cost), fmt_money(mat_cost), fmt_money(other_cost),
        fmt_money(total_cost), fmt_money(net_profit), fmt_money(total_price),
        status, quote_sent, quote_approved, deposit_date, final_payment_date,
        lead_source, ""
    ])

if job_rows:
    print(f"Writing {len(job_rows)} jobs to Job Tracker...")
    script_write_rows(JOB_TRACKER_SCRIPT, "Job Tracker", 5, job_rows)
else:
    print("No jobs to write")

# ── Calculate revenue metrics from invoices ──────────────────
ytd_jobs    = len(jobs)
month_start = today.replace(day=1)
year_start  = today.replace(month=1, day=1)

weekly_collections  = 0.0
monthly_collections = 0.0
ytd_collections     = 0.0

# Jobber PaymentRecord has no date field — use invoice issuedDate as proxy.
# Only count invoices that have at least some payment (paymentsTotal > 0).
for inv in invoices:
    payments_total = float(inv.get("paymentsTotal") or 0)
    if payments_total <= 0:
        continue
    # Use issuedDate as the "payment received" proxy date
    raw_date = (inv.get("issuedDate") or "")[:10]
    if not raw_date:
        continue
    try:
        inv_date = datetime.date.fromisoformat(raw_date)
    except ValueError:
        continue
    if inv_date >= year_start:
        ytd_collections += payments_total
    if inv_date >= month_start:
        monthly_collections += payments_total
    if week_start <= inv_date <= week_end:
        weekly_collections += payments_total

# Monthly profit estimate: subtract costs of paid jobs issued this month
monthly_cost = 0.0
for job in jobs:
    inv_nodes = job.get("invoices", {}).get("nodes", []) or []
    if not inv_nodes:
        continue
    inv = inv_nodes[0]
    inv_status = (inv.get("invoiceStatus") or "").upper()
    if inv_status == "PAID":
        raw = (inv.get("issuedDate") or "")[:10]
        try:
            if raw and datetime.date.fromisoformat(raw) >= month_start:
                job_num = str(job.get("jobNumber", ""))
                line_items = job.get("lineItems", {}).get("nodes", []) or []
                sub_c = sum(float(li.get("unitCost") or 0) * float(li.get("quantity") or 1) for li in line_items)
                mat_c = sum(float(e.get("total") or 0) for e in expenses_by_job.get(job_num, []))
                monthly_cost += sub_c + mat_c
        except ValueError:
            pass

monthly_profit   = monthly_collections - monthly_cost
ytd_profit_final = ytd_revenue - ytd_cost

print(f"Revenue metrics — YTD: ${ytd_collections:.2f} | Month: ${monthly_collections:.2f} | Week: ${weekly_collections:.2f}")
print(f"Profit  metrics — YTD: ${ytd_profit_final:.2f} | Month: ${monthly_profit:.2f}")

# ── Write all metrics to Dashboard!B2:G2 ─────────────────────
# B2=YTD Revenue  C2=Monthly Revenue  D2=Weekly Collections
# E2=YTD Profit   F2=Monthly Profit   G2=Job Count
dashboard_payload = {
    "tab": "Dashboard",
    "range": "B2:G2",
    "values": [[
        round(ytd_collections or ytd_revenue, 2),
        round(monthly_collections, 2),
        round(weekly_collections, 2),
        round(ytd_profit_final, 2),
        round(monthly_profit, 2),
        ytd_jobs
    ]]
}
resp = script_get(JOB_TRACKER_SCRIPT, dashboard_payload)
print(f"Dashboard tab write: {resp[:100]}")

weekly_new_sales = 0
for job in jobs:
    quote = job.get("quote") or {}
    if quote.get("quoteStatus") == "CONVERTED":
        conv = fmt_date(quote.get("transitionedAt",""))
        if conv:
            try:
                d = datetime.date.fromisoformat(conv)
                if week_start <= d <= week_end:
                    weekly_new_sales += float((quote.get("amounts") or {}).get("subtotal") or 0)
            except: pass

print(f"Week {week_start} to {week_end}: collections=${weekly_collections:.2f}, new sales=${weekly_new_sales:.2f}")

# ── Write to WC KBPI ──────────────────────────────────────────
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
    print(f"KBPI row {target_row} updated: {resp[:80]}")
else:
    print(f"Week ending {week_end_str} not found in KBPI tab")

print("Jobber sync complete!")
