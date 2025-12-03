import json
import requests
from metabase_api import Metabase_API

# CONFIG
DEV_URL = 'http://localhost:3001'
DEV_USER = 'skanderbenregaya@gmail.com'
DEV_PASS = 'Motdepasse@21011990'

def inspect():
    print(f"🕵️ Connecting to {DEV_URL}...")
    mb = Metabase_API(DEV_URL, DEV_USER, DEV_PASS)
    
    # 1. Find the Dashboard ID
    dashboards = mb.get('/api/dashboard')
    target_dash = next((d for d in dashboards if d['name'] == "Equipe Analysis"), None)
    
    if not target_dash:
        print("❌ Dashboard 'Equipe Analysis' not found!")
        return

    print(f"✅ Found Dashboard ID: {target_dash['id']}")

    # 2. Get the RAW FULL Details
    # We use requests directly to see the raw JSON without wrapper filtering
    headers = {'Content-Type': 'application/json', 'X-Metabase-Session': mb.session_id}
    res = requests.get(f"{DEV_URL}/api/dashboard/{target_dash['id']}", headers=headers)
    full_data = res.json()

    # 3. Print the Keys to see where cards are
    print("\n--- ROOT KEYS ---")
    print(full_data.keys())

    print("\n--- CHECKING FOR CARDS ---")
    if 'ordered_cards' in full_data:
        print(f"Found 'ordered_cards': {len(full_data['ordered_cards'])} items")
    elif 'dashcards' in full_data:
        print(f"Found 'dashcards': {len(full_data['dashcards'])} items")
    elif 'cards' in full_data:
        print(f"Found 'cards': {len(full_data['cards'])} items")
    else:
        print("❌ NO CARD KEY FOUND! Here is the full dump:")
        print(json.dumps(full_data, indent=2))

if __name__ == "__main__":
    inspect()