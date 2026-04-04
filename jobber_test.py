import os, requests, json

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

print("\n--- Client custom fields ---")
result = q("""
{
  clients(first: 5) {
    nodes {
      id name
      customFields {
        ... on CustomFieldText { label valueText }
        ... on CustomFieldNumeric { label valueNumeric }
        ... on CustomFieldDropdown { label valueDropdown { value label } }
        ... on CustomFieldArea { label valueArea { value } }
      }
    }
  }
}
""")
print(json.dumps(result, indent=2))
