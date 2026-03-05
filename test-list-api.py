import requests

# World Bank API
url = "https://api.worldbank.org/v2/country/TUN/indicator/UNDP.HDI.XD?format=json"

response = requests.get(url)

print("Status:", response.status_code)

data = response.json()

# Les données sont dans l'élément 1
for item in data[1][:10]:  # affiche 10 lignes
    print(
        item["country"]["value"],
        item["date"],
        item["value"]
    )