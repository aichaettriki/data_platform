import json
import os
import requests

# --- CONFIG (DEV SOURCE) ---
DEV_URL = 'http://localhost:3001'
DEV_USER = 'skanderbenregaya@gmail.com'
DEV_PASS = 'Motdepasse@21011990'
OUTPUT_DIR = '../saved_states'

def export_work():
    print(f"💾 Connecting to Dev ({DEV_URL})...")
    
    # 1. Login manually (Bypassing wrapper to handle v0.56+ API)
    session = requests.Session()
    try:
        # Get Session Token
        res = session.post(f"{DEV_URL}/api/session", json={"username": DEV_USER, "password": DEV_PASS})
        res.raise_for_status()
        token = res.json()['id']
        headers = {'Content-Type': 'application/json', 'X-Metabase-Session': token}
    except Exception as e:
        print(f"❌ Login Failed: {e}")
        return
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. Get All Dashboards
    dash_list = session.get(f"{DEV_URL}/api/dashboard", headers=headers).json()
    print(f"📦 Found {len(dash_list)} dashboards.")

    for dash in dash_list:
        print(f"   ... Fetching: {dash['name']}")
        
        # 3. Get Full Details (Deep Fetch)
        full_dash = session.get(f"{DEV_URL}/api/dashboard/{dash['id']}", headers=headers).json()
        
        export_payload = {
            "type": "dashboard",
            "name": full_dash['name'],
            "description": full_dash.get('description'),
            "cards": []
        }
        
        # 4. Extract Cards & Layout (v0.50+ Compatibility)
        # Metabase now puts cards in 'ordered_cards' or 'dashcards'
        cards = full_dash.get('ordered_cards', [])
        if not cards:
            cards = full_dash.get('dashcards', [])

        for item in cards:
            card_def = item.get('card', {})
            
            # CAPTURE LAYOUT (Row, Col, Size)
            layout = {
                "row": item.get('row', 0),
                "col": item.get('col', 0),
                "size_x": item.get('size_x', 4),
                "size_y": item.get('size_y', 3),
            }

            # A. Text Cards
            if not card_def and 'visualization_settings' in item:
                export_payload['cards'].append({
                    "is_text": True,
                    "text": item['visualization_settings'].get('text', ''),
                    "visualization_settings": item['visualization_settings'],
                    "layout": layout
                })
                continue

            # B. Data Cards
            # Try to resolve Table Name
            table_name = None
            if card_def.get('dataset_query', {}).get('type') == 'query':
                try:
                    tid = card_def['dataset_query']['query'].get('source-table')
                    if tid:
                        tbl = session.get(f"{DEV_URL}/api/table/{tid}", headers=headers).json()
                        table_name = f"{tbl['schema']}.{tbl['name']}"
                except: pass

            export_payload['cards'].append({
                "is_text": False,
                "name": card_def.get('name', 'Untitled'),
                "dataset_query": card_def.get('dataset_query'),
                "display": card_def.get('display'),
                "visualization_settings": card_def.get('visualization_settings'),
                "layout": layout,
                "_source_table_name": table_name
            })

        safe_name = dash['name'].replace(" ", "_").lower()
        with open(f"{OUTPUT_DIR}/{safe_name}.json", 'w') as f:
            json.dump(export_payload, f, indent=4)
            print(f"   ✅ Saved {safe_name}.json with {len(export_payload['cards'])} cards")

if __name__ == "__main__":
    export_work()