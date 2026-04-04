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
        return {}
    return data.get("data", {})

# Test all possible PaymentRecord fields
print("\n--- PaymentRecord fields ---")
result = q("""
{
  invoices(first: 3) {
    nodes {
      id invoiceNumber invoiceStatus paymentsTotal
      paymentRecords(first: 5) {
        nodes {
          id
          amount
          type
          createdAt
          adjustedAt
          enteredBy { name }
        }
      }
    }
  }
}
""")
print(result)

# Test job source as enum
print("\n--- Job source enum value ---")
result2 = q("""
{
  jobs(first: 3) {
    nodes {
      jobNumber
      source
      quote {
        quoteNumber
        quoteStatus
        createdAt
        transitionedAt
      }
    }
  }
}
""")
print(result2)

# Test client leadSource
print("\n--- Client leadSource field ---")
result3 = q("""
{
  clients(first: 3) {
    nodes {
      id name
      leadSource { leadSourceName jobberWebUri }
    }
  }
}
""")
print(result3)
