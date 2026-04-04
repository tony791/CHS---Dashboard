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

# Test client custom fields
print("\n--- Client custom fields ---")
result = q("""
{
  clients(first: 5) {
    nodes {
      id name
      customFields {
        ... on CustomFieldText {
          label
          value: valueText
        }
        ... on CustomFieldNumeric {
          label
          value: valueNumeric
        }
        ... on CustomFieldDropdown {
          label
          value: valueLabel
        }
      }
    }
  }
}
""")
print(result)
