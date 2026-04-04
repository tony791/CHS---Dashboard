import os, requests

CLIENT_ID = os.environ['JOBBER_CLIENT_ID']
CLIENT_SECRET = os.environ['JOBBER_CLIENT_SECRET']
REFRESH_TOKEN = os.environ['JOBBER_REFRESH_TOKEN']

token_resp = requests.post(
    "https://api.getjobber.com/api/oauth/token",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    data={"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
          "grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN})
ACCESS_TOKEN = token_resp.json()["access_token"]
print("Token OK ✓")

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
    "X-JOBBER-GRAPHQL-VERSION": "2025-04-16"
}

def q(query):
    r = requests.post("https://api.getjobber.com/api/graphql", headers=HEADERS, json={"query": query})
    data = r.json()
    if "errors" in data:
        print(f"Errors: {data['errors']}")
    return data.get("data", {})

# Test 1: Quote fields
print("\n--- Quote fields test ---")
result = q("""
{
  quotes(first: 3) {
    nodes {
      id quoteNumber quoteStatus
      createdAt sentAt
      approvedAt
      client { name }
      amounts { subtotal }
    }
  }
}
""")
print(result)

# Test 2: Client referral source
print("\n--- Client referral source ---")
result2 = q("""
{
  clients(first: 3) {
    nodes {
      id name
      leadsource: leadsource
    }
  }
}
""")
print(result2)

# Test 3: Payments query
print("\n--- Payments query ---")
result3 = q("""
{
  payments(first: 5) {
    nodes {
      id amount receivedAt
      paymentType
      invoice { id invoiceNumber total client { name } }
    }
  }
}
""")
print(result3)
