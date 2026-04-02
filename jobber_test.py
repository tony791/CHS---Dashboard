import os, requests

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']

# Refresh token
token_resp = requests.post(
    "https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN
    })

token_data = token_resp.json()
ACCESS_TOKEN = token_data["access_token"]
print(f"Token OK: {ACCESS_TOKEN[:20]}...")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "X-JOBBER-GRAPHQL-VERSION": "2025-04-16"
}

# Test 1: Try ALL job statuses
print("\n--- Testing jobs query with all statuses ---")
r = requests.post("https://api.getjobber.com/api/graphql",
    headers=HEADERS,
    json={"query": "{ jobs(first: 5) { nodes { id jobNumber title } totalCount } }"})
print(f"HTTP: {r.status_code}")
print(f"Response: {r.text[:500]}")

# Test 2: Try invoices
print("\n--- Testing invoices ---")
r2 = requests.post("https://api.getjobber.com/api/graphql",
    headers=HEADERS,
    json={"query": "{ invoices(first: 5) { nodes { id invoiceNumber total status } totalCount } }"})
print(f"HTTP: {r2.status_code}")
print(f"Response: {r2.text[:500]}")

# Test 3: Account info
print("\n--- Account info ---")
r3 = requests.post("https://api.getjobber.com/api/graphql",
    headers=HEADERS,
    json={"query": "{ account { id name } }"})
print(f"HTTP: {r3.status_code}")
print(f"Response: {r3.text[:300]}")
