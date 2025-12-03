import json
import os
import requests
from metabase_api import Metabase_API

# --- CONFIG (DEV SOURCE) ---
DEV_URL = 'http://localhost:3001'
DEV_USER = 'skanderbenregaya@gmail.com'
DEV_PASS = 'Motdepasse@21011990'
OUTPUT_DIR = '../saved_states'

def export_work():
    print(f"💾 Connecting to Dev ({DEV_URL})...")
    try:
        mb = Metabase_API(DEV_URL, DEV_USER, DEV_PASS)
    except Exception as e:
        print(f"❌ Could not connect: {e}")
        return
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # --- 1. EXPORT ALL DASHBOARDS ---
    dashboards = mb.get('/api/dashboard')
    print(f"📦 Found {len(dashboards)} dashboards.")

    for dash in dashboards:
        full_dash = mb.get(f'/api/dashboard/{dash["id"]}')
        
        export_payload = {
            "type": "dashboard",
            "name": full_dash['name'],
            "description": full_dash.get('description'),
            "cards": []
        }
        
        for card_dash in full_dash.get('ordered_cards', []):
            card_def = card_dash.get('card', {})
            
            # Capture Layout (CRITICAL FOR VISIBILITY)
            layout = {
                "row": card_dash.get('row', 0),
                "col": card_dash.get('col', 0),
                "size_x": card_dash.get('size_x', 4),
                "size_y": card_dash.get('size_y', 3),
            }

            # Handle Text Cards (Markdown)
            if not card_def and 'visualization_settings' in card_dash:
                export_payload['cards'].append({
                    "is_text": True,
                    "text": card_dash['visualization_settings'].get('text', ''),
                    "layout": layout
                })
                continue

            # Handle Data Cards
            # Enrich with Table Name for robust mapping
            table_name = None
            if card_def.get('dataset_query', {}).get('type') == 'query':
                tid = card_def['dataset_query']['query'].get('source-table')
                if tid:
                    try:
                        # Fetch table metadata to get "schema.table_name"
                        tbl = mb.get(f"/api/table/{tid}")
                        if 'schema' in tbl and 'name' in tbl:
                            table_name = f"{tbl['schema']}.{tbl['name']}"
                    except:
                        pass

            export_payload['cards'].append({
                "is_text": False,
                "name": card_def['name'],
                "dataset_query": card_def['dataset_query'],
                "display": card_def['display'],
                "visualization_settings": card_def['visualization_settings'],
                "layout": layout,
                "_source_table_name": table_name # Saved for mapping later
            })

        safe_name = dash['name'].replace(" ", "_").lower()
        with open(f"{OUTPUT_DIR}/dash_{safe_name}.json", 'w') as f:
            json.dump(export_payload, f, indent=4)
            print(f"   ✅ Exported Dashboard: {dash['name']}")

    # --- 2. EXPORT STANDALONE QUESTIONS (Optional) ---
    # This grabs questions that might not be on a dashboard
    print("📦 Exporting Standalone Questions...")
    cards = mb.get('/api/card')
    for card in cards:
        if card.get('archived', False): continue
        
        # Only export if we haven't already (simplification: just dump all interesting ones)
        # We save them as individual files
        safe_card = card['name'].replace(" ", "_").lower()
        
        # Resolve table name
        table_name = None
        if card.get('dataset_query', {}).get('type') == 'query':
            tid = card['dataset_query']['query'].get('source-table')
            if tid:
                try:
                    tbl = mb.get(f"/api/table/{tid}")
                    table_name = f"{tbl['schema']}.{tbl['name']}"
                except: pass

        payload = {
            "type": "card",
            "name": card['name'],
            "dataset_query": card['dataset_query'],
            "display": card['display'],
            "visualization_settings": card['visualization_settings'],
            "_source_table_name": table_name
        }
        
        with open(f"{OUTPUT_DIR}/card_{safe_card}.json", 'w') as f:
            json.dump(payload, f, indent=4)

    print(f"✅ Export Complete!")

if __name__ == "__main__":
    export_work()