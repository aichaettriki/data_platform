import requests
import pandas as pd
from io import StringIO

# ─────────────────────────────────────────────────────────
# URL officielle du CSV PNUD — toutes les années, tous les pays
# ─────────────────────────────────────────────────────────
CSV_URL = "https://hdr.undp.org/sites/default/files/2025_HDR/HDR25_Composite_indices_complete_time_series.csv"

PAYS_CIBLES = ["Tunisia", "Morocco"]

# ─────────────────────────────────────────────────────────
# ETAPE 1 — Téléchargement
# ─────────────────────────────────────────────────────────
print("Telechargement du fichier CSV officiel PNUD...")
response = requests.get(CSV_URL, timeout=60)
response.raise_for_status()
print(f"OK — {len(response.content) / 1024:.0f} KB telecharges")

# ─────────────────────────────────────────────────────────
# ETAPE 2 — Lecture du CSV
# ─────────────────────────────────────────────────────────
df = pd.read_csv(StringIO(response.text))
print(f"\nColonnes disponibles :")
print([c for c in df.columns.tolist()])
print(f"\nNombre de pays : {df['country'].nunique()}")

# ─────────────────────────────────────────────────────────
# ETAPE 3 — Filtrer Tunisie et Maroc
# ─────────────────────────────────────────────────────────
df_pays = df[df["country"].isin(PAYS_CIBLES)].copy()
print(f"\nLignes trouvees pour {PAYS_CIBLES} : {len(df_pays)}")

# ─────────────────────────────────────────────────────────
# ETAPE 4 — Identifier les colonnes HDI (hdi_YYYY)
# ─────────────────────────────────────────────────────────
hdi_cols = [c for c in df.columns if c.startswith("hdi_") and c[4:].isdigit()]
print(f"\nAnnees HDI disponibles : {sorted(hdi_cols)}")

# ─────────────────────────────────────────────────────────
# ETAPE 5 — Restructurer en lignes (une ligne par pays/année)
# ─────────────────────────────────────────────────────────
records = []
for _, row in df_pays.iterrows():
    for col in hdi_cols:
        year = int(col.replace("hdi_", ""))
        value = row[col]
        if pd.isna(value):
            continue
        records.append({
            "country_code": row.get("iso3", row.get("country_code", "")),
            "country_name": row["country"],
            "year":         year,
            "HDI_value":    float(value),
            "source":       "UNDP Human Development Report — hdr.undp.org",
        })

# ─────────────────────────────────────────────────────────
# ETAPE 6 — Affichage
# ─────────────────────────────────────────────────────────
print(f"\nTOTAL : {len(records)} enregistrements\n")
print(f"{'country_code':<15} {'country_name':<12} {'year':<8} {'HDI_value':<12} source")
print("-" * 70)
for r in records:
    print(f"{r['country_code']:<15} {r['country_name']:<12} {r['year']:<8} {r['HDI_value']:<12} {r['source']}")

# ─────────────────────────────────────────────────────────
# ETAPE 7 — Sauvegarde CSV
# ─────────────────────────────────────────────────────────
import json
with open("hdi_pnud.json", "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)
print(f"\nSauvegarde dans hdi_pnud.json")