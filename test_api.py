import requests
import json

API_URL = "https://unstats.un.org/sdgapi/v1/sdg/Indicator/Data?areaCode=788&pageSize=5"

PAYS = {
    "788": "Tunisie",
    "504": "Maroc",
}

tous_records = []

for country_code, country_name in PAYS.items():
    print(f"\nAppel API pour {country_name}...")

    response = requests.get(API_URL, params={
        "areaCode":   country_code,
        "pageSize":   800,
    }, timeout=30)

    data = response.json()
    observations = data.get("data", [])
    print(f"  {len(observations)} observations trouvées")

    for obs in observations:
        record = {
            "country_code": country_code,
            "country_name": country_name,
            "year":         obs.get("timePeriodStart"),
            "HDI_value":    obs.get("value"),
            "source":       obs.get("source"),
        }
        tous_records.append(record)
        print(f"  → {record['country_name']} | {record['year']} | {record['HDI_value']}")

print(f"\nTOTAL : {len(tous_records)} enregistrements")

with open("sdg_hdi.json", "w", encoding="utf-8") as f:
    json.dump(tous_records, f, ensure_ascii=False, indent=2)

print("Sauvegardé dans sdg_hdi.json")