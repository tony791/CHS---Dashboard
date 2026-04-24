import os, requests, datetime, sys, json, time, base64
from nacl import encoding, public

# ── Required env vars ─────────────────────────────────────────
CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']
GOOGLE_API_KEY = os.environ['GOOGLE_API_KEY']
JOB_TRACKER_SHEET_ID = os.environ['JOB_TRACKER_SHEET_ID']
WC_SHEET_ID = os.environ['WC_SHEET_ID']

# ── Optional env vars ─────────────────────────────────────────
# PAT with Secrets read+write on this repo. When set, we persist the rotated
# refresh token back to GitHub so the next run doesn't 401.
GH_TOKEN_ROTATOR = os.environ.get('GH_TOKEN_ROTATOR')
# Auto-populated by GitHub Actions. Format: "owner/repo".
GITHUB_REPOSITORY = os.environ.get('GITHUB_REPOSITORY', '')

JOB_TRACKER_SCRIPT = "https://script.google.com/macros/s/AKfycbwSatQFSlvP8GY0XXMRjgdN8esc5IJo4T4cTj1kwBShjJ_iRLOc3fnGZ-ezfGbNx5nU/exec"
WC_TRACKER_SCRIPT  = "https://script.google.com/macros/s/AKfycbzN2yakVRoBYdSa-F-UreDLzl8YctxvCt1vCAqPJEk8kGSIKD8ak-qkSRJwpjNCd_Gm/exec"

JOBBER_API = "https://api.getjobber.com/api/graphql"

# Hard cap on total jobs to fetch (50 pages × 100 jobs = 5,000 jobs max).
# Bumped when we cross 5k lifetime jobs (good problem to have).
MAX_JOBS_TO_FETCH = 5000
JOBS_PAGE_SIZE = 100


# ── GitHub secret rotation ────────────────────────────────────
def _encrypt_secret(public_key_b64, secret_value):
    """Encrypt a value using the repo's libsodium public key."""
    public_key = public.PublicKey(public_key_b64.encode("utf-8"), encoding.Base64Encoder())
    sealed_box = public.SealedBox(public_key)
    encrypted = sealed_box.encrypt(secret_value.encode("utf-8"))
    return base64.b64encode(encrypted).decode("utf-8")


def update_github_secret(secret_name, secret_value):
    """Persist a rotated value back into GitHub Actions secrets. Returns True on success."""
    if not GH_TOKEN_ROTATOR:
        print(f"  !! GH_TOKEN_ROTATOR not configured; cannot persist {secret_name}")
        print(f"  !! You will need to manually update this secret before the next run.")
        return False
    if not GITHUB_REPOSITORY or '/' not in GITHUB_REPOSITORY:
        print(f"  !! GITHUB_REPOSITORY not set; cannot determine repo for secret update")
        return False

    headers = {
        "Authorization": f"Bearer {GH_TOKEN_ROTATOR}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    key_url = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/secrets/public-key"
    key_resp = requests.get(key_url, headers=headers, timeout=30)
    if key_resp.status_code != 200:
        print(f"  !! Failed to fetch public key: {key_resp.status_code} {key_resp.text[:200]}")
        return False

    public_key_data = key_resp.json()
    try:
        encrypted_value = _encrypt_secret(public_key_data["key"], secret_value)
    except Exception as e:
        print(f"  !! Encryption failed: {e}")
        return False

    secret_url = f"https://api.github.com/repos/{GITHUB_REPOSITORY}/actions/secrets/{secret_name}"
    update_resp = requests.put(
        secret_url,
        headers=headers,
        json={"encrypted_value": encrypted_value, "key_id": public_key_data["key_id"]},
        timeout=30,
    )
    if update_resp.status_code not in (201, 204):
        print(f"  !! Secret update failed: {update_resp.status_code} {update_resp.text[:200]}")
        return False
    return True


# ── Token refresh (with rotation persistence) ─────────────────
print("Refreshing Jobber access token...")
token_resp = requests.post(
    "https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
    },
    timeout=30,
)
print(f"Token refresh HTTP: {token_resp.status_code}")

try:
    token_data = token_resp.json()
except ValueError:
    print(f"!! Token response was not JSON. Raw body: {token_resp.text[:500]}")
    sys.exit(1)

if "access_token" not in token_data:
    print(f"!! Token refresh failed: {token_data}")
    sys.exit(1)

ACCESS_TOKEN = token_data["access_token"]
NEW_REFRESH_TOKEN = token_data.get("refresh_token")
print("Access token refreshed ✓")

if NEW_REFRESH_TOKEN and NEW_REFRESH_TOKEN != REFRESH_TOKEN:
    print("Persisting rotated refresh token to GitHub secret JOBBER_REFRESH_TOKEN...")
    if update_github_secret("JOBBER_REFRESH_TOKEN", NEW_REFRESH_TOKEN):
        print("Refresh token rotated and persisted ✓")
    else:
        # Exit hard — running the full sync with a token we can't persist means the
        # next hourly run will 401 and we'll lose data continuity. Safer to fail now
        # and let a human intervene.
        print("!! CRITICAL: Could not persist rotated refresh token. Aborting sync.")
        print("!! Manually update JOBBER_REFRESH_TOKEN in repo secrets before retrying.")
        sys.exit(1)
else:
    print("Refresh token unchanged (no rotation this run).")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "X-JOBBER-GRAPHQL-VERSION": "2025-04-16",
}


def jobber_query(query, max_retries=5):
    """Run a GraphQL query. Retries on THROTTLED errors.

    Prefers Jobber's own throttleStatus.restoreRate + requestedQueryCost hints
    when present (tells us exactly how long credits need to refill). Falls back
    to exponential backoff otherwise.

    Always returns a dict (possibly empty) — never None.
    """
    backoff = 15
    for attempt in range(max_retries):
        try:
            r = requests.post(JOBBER_API, headers=HEADERS, json={"query": query}, timeout=60)
        except requests.RequestException as e:
            print(f"Jobber request failed: {e}")
            return {}
        if r.status_code != 200:
            print(f"Jobber API error: {r.status_code} {r.text[:200]}")
            return {}
        try:
            data = r.json()
        except ValueError:
            print(f"Jobber returned non-JSON: {r.text[:200]}")
            return {}
        errors = data.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors
        )
        if throttled and attempt < max_retries - 1:
            # Try to parse Jobber's throttleStatus hint from the first throttled error.
            wait_s = backoff
            for err in errors:
                ext = err.get("extensions") or {}
                cost = ext.get("cost") or {}
                status = cost.get("throttleStatus") or {}
                requested = cost.get("requestedQueryCost") or 0
                currently = status.get("currentlyAvailable") or 0
                restore = status.get("restoreRate") or 0
                if requested and restore and requested > currently:
                    hint = (requested - currently) / restore
                    # Add 25% buffer; cap at 5 min to avoid runaway waits.
                    wait_s = min(int(hint * 1.25) + 2, 300)
                    break
            print(f"  Throttled by Jobber, waiting {wait_s}s before retry {attempt + 2}/{max_retries}...")
            time.sleep(wait_s)
            backoff *= 2
            continue
        if errors:
            print(f"GraphQL errors: {errors}")
        return data.get("data") or {}
    print("  Exhausted throttle retries — returning empty result")
    return {}


def safe_nodes(container, *keys):
    """Walk a nested dict and return .nodes as a list, surviving None anywhere."""
    cur = container
    for k in keys:
        if not isinstance(cur, dict):
            return []
        cur = cur.get(k)
        if cur is None:
            return []
    if isinstance(cur, dict):
        return cur.get("nodes") or []
    return []


def script_get(script_url, payload):
    try:
        r = requests.get(script_url, params={"payload": json.dumps(payload)}, timeout=60)
        return r.text[:300]
    except Exception as ex:
        return f"Error: {ex}"


def script_write_rows(script_url, tab, start_row, values, clear_end_row=60):
    clear_payload = {
        "tab": tab,
        "clear": True,
        "clearRange": f"A{start_row}:R{clear_end_row}",
        "range": f"A{start_row}:R{clear_end_row}",
        "values": [["", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]],
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
    r = requests.get(url, timeout=30)
    return r.json().get("values", [])


def fmt_date(dt_str):
    if not dt_str:
        return ""
    try:
        return dt_str[:10]
    except Exception:
        return ""


def fmt_money(val):
    return f"${float(val or 0):.2f}"


today = datetime.date.today()
current_year = today.year
current_year_prefix = str(current_year)
week_start = today - datetime.timedelta(days=(today.weekday() + 1) % 7)
week_end = week_start + datetime.timedelta(days=6)


# ── Fetch jobs (paginated) ────────────────────────────────────
print(f"Fetching jobs (up to {MAX_JOBS_TO_FETCH})...")
jobs_all = []
cursor = None
page = 0

while True:
    page += 1
    after_arg = f', after: "{cursor}"' if cursor else ""
    query = """
{
  jobs(first: %d%s) {
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
    pageInfo { hasNextPage endCursor }
    totalCount
  }
}
""" % (JOBS_PAGE_SIZE, after_arg)

    data = jobber_query(query)
    jobs_block = data.get("jobs") or {}
    nodes = jobs_block.get("nodes") or []
    jobs_all.extend(nodes)

    page_info = jobs_block.get("pageInfo") or {}
    has_next = bool(page_info.get("hasNextPage"))
    cursor = page_info.get("endCursor")

    print(f"  Page {page}: +{len(nodes)} jobs (total so far: {len(jobs_all)})")

    if not has_next or not cursor:
        break
    if len(jobs_all) >= MAX_JOBS_TO_FETCH:
        print(f"  !! Hit MAX_JOBS_TO_FETCH cap of {MAX_JOBS_TO_FETCH}. Increase the cap or add filters.")
        break

jobs = [j for j in jobs_all if (j.get("createdAt") or "").startswith(current_year_prefix)]
print(f"Found {len(jobs_all)} total jobs, {len(jobs)} from {current_year}")


# ── Fetch expenses per current-year job ────────────────────────
expenses_by_job = {}
print("Fetching expenses per job...")
for job in jobs:
    job_id = job.get("id", "")
    job_num = str(job.get("jobNumber", ""))
    exp_data = jobber_query("""
{
  job(id: "%s") {
    expenses(first: 50) {
      nodes { title description total }
    }
  }
}
""" % job_id)
    expenses = safe_nodes(exp_data, "job", "expenses")
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
invoices = safe_nodes(invoices_data, "invoices")
print(f"Found {len(invoices)} invoices")


# ── Build Job Tracker rows ─────────────────────────────────────
print("Building Job Tracker data...")
job_rows = []
ytd_revenue = 0.0
ytd_cost = 0.0

for job in jobs:
    job_num = str(job.get("jobNumber", ""))
    total_price = float(job.get("total") or 0)
    job_status_raw = (job.get("jobStatus") or "").lower()
    client = job.get("client") or {}
    client_name = client.get("name", "")

    prop = job.get("property") or {}
    addr = prop.get("address") or {}
    street = addr.get("street", "") or ""
    city = addr.get("city", "") or ""
    address = f"{street}, {city}".strip(", ")

    phones = client.get("phones") or []
    emails_list = client.get("emails") or []
    phone = phones[0].get("number", "") if phones else ""
    email = emails_list[0].get("address", "") if emails_list else ""

    custom_fields = client.get("customFields") or []
    referred_by = ""
    for cf in custom_fields:
        if cf.get("label", "").lower() in ["referred by", "referral", "lead source"]:
            referred_by = cf.get("valueText", "") or cf.get("valueDropdown", "") or ""
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

    line_items = safe_nodes(job, "lineItems")
    sub_cost = sum(float(li.get("unitCost") or 0) * float(li.get("quantity") or 1) for li in line_items)
    expenses = expenses_by_job.get(job_num, []) or []
    mat_cost = sum(float(e.get("total") or 0) for e in expenses)
    other_cost = 0.0
    total_cost = sub_cost + mat_cost + other_cost
    net_profit = total_price - total_cost

    job_invoices = safe_nodes(job, "invoices")
    inv = job_invoices[0] if job_invoices else {}
    inv_status = (inv.get("invoiceStatus", "") or "").upper()
    payments_total = float(inv.get("paymentsTotal") or 0)
    deposit_amt = float((inv.get("amounts") or {}).get("depositAmount") or 0)

    payment_records = safe_nodes(job, "paymentRecords")
    num_payments = len(payment_records)
    deposit_date = ""
    final_payment_date = ""
    if deposit_amt > 0 and num_payments >= 1:
        deposit_date = fmt_date(inv.get("issuedDate", ""))
    if inv_status == "PAID":
        final_payment_date = fmt_date(inv.get("dueDate", ""))

    quote = job.get("quote") or {}
    quote_sent = fmt_date(quote.get("createdAt", ""))
    quote_status = (quote.get("quoteStatus", "") or "").lower()
    quote_approved = fmt_date(quote.get("transitionedAt", "")) if quote_status in ["approved", "converted"] else ""

    visits = job.get("visits") or {}
    has_visits = (visits.get("totalCount") or 0) > 0
    if inv_status == "PAID":
        status = "Completed"
    elif inv_status in ["SENT", "VIEWED", "PAST_DUE"]:
        status = "Awaiting Payment"
    elif job_status_raw in ["requires_invoicing"]:
        status = "Awaiting Payment"
    elif job_status_raw in ["completed", "archived"]:
        status = "Completed"
    elif has_visits:
        status = "In Progress"
    else:
        status = "Need to Schedule"

    ytd_revenue += total_price
    ytd_cost += total_cost

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
ytd_jobs = len(jobs)
month_start = today.replace(day=1)
year_start = today.replace(month=1, day=1)

weekly_collections = 0.0
monthly_collections = 0.0
ytd_collections = 0.0

# Jobber PaymentRecord has no date field — using invoice.issuedDate as proxy.
# Only count invoices that actually received some payment.
for inv in invoices:
    payments_total = float(inv.get("paymentsTotal") or 0)
    if payments_total <= 0:
        continue
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

monthly_cost = 0.0
for job in jobs:
    inv_nodes = safe_nodes(job, "invoices")
    if not inv_nodes:
        continue
    inv = inv_nodes[0]
    inv_status = (inv.get("invoiceStatus") or "").upper()
    if inv_status != "PAID":
        continue
    raw = (inv.get("issuedDate") or "")[:10]
    try:
        if raw and datetime.date.fromisoformat(raw) >= month_start:
            job_num = str(job.get("jobNumber", ""))
            line_items = safe_nodes(job, "lineItems")
            sub_c = sum(float(li.get("unitCost") or 0) * float(li.get("quantity") or 1) for li in line_items)
            mat_c = sum(float(e.get("total") or 0) for e in expenses_by_job.get(job_num, []) or [])
            monthly_cost += sub_c + mat_c
    except ValueError:
        pass

monthly_profit = monthly_collections - monthly_cost
ytd_profit_final = ytd_revenue - ytd_cost

print(f"Revenue metrics — YTD: ${ytd_collections:.2f} | Month: ${monthly_collections:.2f} | Week: ${weekly_collections:.2f}")
print(f"Profit  metrics — YTD: ${ytd_profit_final:.2f} | Month: ${monthly_profit:.2f}")


# ── Write metrics to KPI_Sync!B2:G2 ──────────────────────────
# Dedicated tab for sync output — keeps the "Dashboard" pretty view untouched.
# B2=YTD Revenue  C2=Monthly Revenue  D2=Weekly Collections
# E2=YTD Profit   F2=Monthly Profit   G2=Job Count
#
# Safety guard: if the jobs query failed (e.g. throttled) we'd end up writing
# zeroes for YTD Profit, Monthly Profit, and Job Count — silently corrupting
# the dashboard. Skip the write in that case so the prior good values stay.
if len(jobs_all) == 0:
    print("!! Skipping KPI_Sync!B2:G2 write — jobs query returned 0 results.")
    print("!! (This usually means throttling or an API issue. Prior values preserved.)")
else:
    dashboard_payload = {
        "tab": "KPI_Sync",
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
    print(f"KPI_Sync tab write: {resp[:100]}")


# ── Weekly new sales ──────────────────────────────────────────
weekly_new_sales = 0
for job in jobs:
    quote = job.get("quote") or {}
    if quote.get("quoteStatus") == "CONVERTED":
        conv = fmt_date(quote.get("transitionedAt", ""))
        if conv:
            try:
                d = datetime.date.fromisoformat(conv)
                if week_start <= d <= week_end:
                    weekly_new_sales += float((quote.get("amounts") or {}).get("subtotal") or 0)
            except Exception:
                pass

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
